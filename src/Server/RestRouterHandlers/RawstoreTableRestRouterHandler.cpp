#include "RawstoreTableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
}

std::map<String, std::map<String, String> > RawstoreTableRestRouterHandler::create_schema = {
    {"required",{
                        {"name","string"}
                }
    },
    {"optional", {
                        {"shards", "int"},
                        {"replication_factor", "int"},
                        {"order_by_granularity", "string"},
                        {"partition_by_granularity", "string"},
                        {"ttl_expression", "string"}
                }
    }
};

void RawstoreTableRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;

    for (const auto & table : tables)
    {
        /// FIXME : Later based on engin seting distinguish rawstore
        std::cout << "table->create_table_query : " << table->create_table_query << std::endl;
        if (table->create_table_query.find("`_raw` String COMMENT 'rawstore'") != String::npos)
        {
            Poco::JSON::Object table_mapping_json;

            const String & query = table->create_table_query;
            const auto & query_ptr = parseQuerySyntax(query);
            const auto & create = query_ptr->as<const ASTCreateQuery &>();

            table_mapping_json.set("name", table->name);
            table_mapping_json.set("order_by_expression", table->sorting_key);
            table_mapping_json.set("partition_by_expression", table->partition_key);
            if(create.storage->ttl_table)
            {
                String ttl = queryToString(*create.storage->ttl_table);
                table_mapping_json.set("ttl", ttl);
            }

            buildColumnsJSON(table_mapping_json, create.columns_list);
            tables_mapping_json.add(table_mapping_json);
        }
    }

    resp.set("data", tables_mapping_json);
}

bool RawstoreTableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
    {
        return false;
    }

    return TableRestRouterHandler::validatePost(payload, error_msg);
}

String RawstoreTableRestRouterHandler::getOrderByExpr(
    const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/, const String & default_order_by_granularity) const
{
    const auto & order_by_granularity = getStringValueFrom(payload, "order_by_granularity", default_order_by_granularity);
    return granularity_func_mapping[order_by_granularity] + ", sourcetype";
}

String RawstoreTableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    std::vector<String> columns_definition;
    columns_definition.push_back("`_raw` String COMMENT 'rawstore'");
    columns_definition.push_back("`_time` DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4)");
    columns_definition.push_back("`_index_time` DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4)");
    columns_definition.push_back("`sourcetype` LowCardinality(String)");
    columns_definition.push_back("`source` String");
    columns_definition.push_back("`host` String");

    return boost::algorithm::join(columns_definition, ",");
}

String RawstoreTableRestRouterHandler::getDefaultPartitionGranularity() const
{
    return "D";
}

String RawstoreTableRestRouterHandler::getDefaultOrderByGranularity() const
{
    return "m";
}

}
