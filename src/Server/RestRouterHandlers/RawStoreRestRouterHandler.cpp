#include "RawStoreRestRouterHandler.h"

#include "SchemaValidator.h"

#include <Core/Block.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <vector>

namespace DB
{
void RawStoreRestRouterHandler::buildColumnsJson(Poco::JSON::Object & resp_table, const String & create_table_info) const
{
    Poco::JSON::Array columns_mapping_json;

    String::size_type create_pos = String::npos;
    String::size_type engine_pos = String::npos;
    create_pos = create_table_info.find_first_of("(");
    engine_pos = create_table_info.find(") ENGINE");

    if (create_pos != String::npos && engine_pos != String::npos)
    {
        const String & columns_info = create_table_info.substr(create_pos + 1, engine_pos - create_pos - 1);

        Strings columns;
        boost::split(columns, columns_info, boost::is_any_of(","));
        for (auto & column : columns)
        {
            Poco::JSON::Object cloumn_mapping_json;

            int index = 0;
            Strings column_elements;
            boost::trim(column);
            boost::split(column_elements, column, boost::is_any_of(" "));
            cloumn_mapping_json.set("name", boost::algorithm::erase_all_copy(column_elements[index], "`"));

            int size = column_elements.size();

            /// extract type
            if (++index < size)
            {
                const String & type = column_elements[index];
                if (type.find("Nullable") != String::npos)
                {
                    cloumn_mapping_json.set("type", type.substr(type.find("(") + 1, type.find(")") - type.find("(") - 1));
                    cloumn_mapping_json.set("nullable", true);
                }
                else
                {
                    cloumn_mapping_json.set("type", type);
                    cloumn_mapping_json.set("nullable", false);
                }
            }

            /// extract default or alias
            if (++index < size)
            {
                const String & element = column_elements[index];
                if (element == "DEFAULT")
                {
                    cloumn_mapping_json.set("default", column_elements[++index]);
                }
                else if (element == "ALIAS")
                {
                    cloumn_mapping_json.set("alias", column_elements[++index]);
                }
                else
                {
                    index--;
                }
            }

            /// extract compression_codec
            if (++index < size)
            {
                const String & compression_codec = column_elements[index];

                if (compression_codec.find("CODEC") != String::npos)
                {
                    cloumn_mapping_json.set(
                        "compression_codec",
                        compression_codec.substr(
                            compression_codec.find("(") + 1, compression_codec.find(")") - compression_codec.find("(")));
                }
                else
                {
                    index--;
                }
            }

            /// FIXME : ttl_expression

            /// FIXME : skipping_index_expression

            columns_mapping_json.add(cloumn_mapping_json);
        }
    }

    resp_table.set("columns", columns_mapping_json);
}

void RawStoreRestRouterHandler::buildTablesJson(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;

    for (const auto & table : tables)
    {
        /// Distinguish rawstore
        if (table->create_table_query.find("`_raw` String COMMENT 'rawstore'") != String::npos)
        {
            Poco::JSON::Object table_mapping_json;

            table_mapping_json.set("name", table->name);
            table_mapping_json.set("engine", table->engine);
            table_mapping_json.set("order_by_expression", table->sorting_key);

            /// extract ttl
            String::size_type ttl_pos = String::npos;
            ttl_pos = table->engine_full.find(" TTL ");
            if (ttl_pos != String::npos)
            {
                String::size_type settings_pos = String::npos;
                settings_pos = table->engine_full.find(" SETTINGS ");
                const String & ttl = table->engine_full.substr(ttl_pos + 5, settings_pos - ttl_pos - 5);
                table_mapping_json.set("ttl_expression", ttl);
            }

            buildColumnsJson(table_mapping_json, table->create_table_query);
            tables_mapping_json.add(table_mapping_json);
        }
    }

    resp.set("data", tables_mapping_json);
}

String RawStoreRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status */) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const String & database_name = getPathParameter("database");
    const auto & tables = catalog_service.findTableByDB(database_name);

    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getClientInfo().initial_query_id);

    buildTablesJson(resp, tables);
    std::stringstream resp_str_stream;
    resp.stringify(resp_str_stream, 4);
    String resp_str = resp_str_stream.str();

    return resp_str;
}

String RawStoreRestRouterHandler::getOrderbyExpr(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & order_by_granularity = payload->has("order_by_granularity") ? payload->get("order_by_granularity").toString() : "m";
    return granularity_func_mapping[order_by_granularity] + ",  sourcetype";
}

String RawStoreRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const
{
    const auto & database_name = getPathParameter("database");

    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back("_raw String COMMENT 'rawstore',");
    create_segments.push_back("_time DateTime64(3) CODEC (DoubleDelta, LZ4), ");
    create_segments.push_back("_index_time DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4), ");
    create_segments.push_back("sourcetype LowCardinality(String), ");
    create_segments.push_back("source String, ");
    create_segments.push_back("host String ");
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = " + getEngineExpr(payload));
    create_segments.push_back("PARTITION BY " + getPartitionExpr(payload, "D"));
    create_segments.push_back("ORDER BY (" + getOrderbyExpr(payload) + ")");

    if (payload->has("ttl_expression"))
    {
        /// FIXME  Enforce time based TTL only
        create_segments.push_back("TTL " + payload->get("ttl_expression").toString());
    }

    if (!shard.empty())
    {
        create_segments.push_back("SETTINGS shard=" + shard);
    }

    return boost::algorithm::join(create_segments, " ");
}
}
