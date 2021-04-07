#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Interpreters/executeQuery.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int CONFIG_ERROR;
    extern const int OK;
}

std::map<String, std::map<String, String> > TableRestRouterHandler::create_schema = {
    {"required",{
                    {"name","string"},
                    {"order_by_expression", "string"},
                    {"_time_column", "string"},
                    {"columns", "array"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {"replication_factor", "int"},
                    {"partition_by_expression", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > TableRestRouterHandler::column_schema = {
    {"required",{
                    {"name","string"},
                    {"type", "string"},
                }
    },
    {"optional", {
                    {"nullable", "bool"},
                    {"default", "string"},
                    {"compression_codec", "string"},
                    {"ttl_expression", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > TableRestRouterHandler::update_schema = {
    {"required",{
                }
    },
    {"optional", {
                    {"ttl_expression", "string"}
                }
    }
};

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
    {
        return false;
    }

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (auto & col : *columns)
    {
        if (!validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>(), error_msg))
        {
            return false;
        }
    }
    return true;
}

bool TableRestRouterHandler::validateGet(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const
{
    return true;
}

bool TableRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(update_schema, payload, error_msg);
}

String TableRestRouterHandler::getTableCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard ) const
{
    String database_name = getPathParameter("database");
    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back(getColumnsDefinition(payload->getArray("columns"), payload->get("_time_column").toString()));
    create_segments.push_back(")");
    create_segments.push_back(fmt::format(
        "ENGINE = DistributedMergeTree({}, {}, {})",
        payload->get("replication_factor").toString(),
        payload->get("shards").toString(),
        payload->get("shard_by_expression").toString()));
    create_segments.push_back("PARTITION BY " + payload->get("partition_by_expression").toString());
    create_segments.push_back("ORDER BY " + payload->get("order_by_expression").toString());
    if (payload->has("ttl_expression"))
    {
        create_segments.push_back("TTL " + payload->get("ttl_expression").toString());
    }
    if (!shard.empty())
    {
        create_segments.push_back("SETTINGS shard=" + shard);
    }

    return boost::algorithm::join(create_segments, " ");
}

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status*/) const
{
    String query = "show databases";
    return processQuery(query);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const auto & params = query_context.getQueryParameters();
    const String shard = params.contains("shard") ? params.at("shard") : String();
    if (params.contains("query_id"))
    {
        query_context.setCurrentQueryId(params.at("query_id"));
    }
    const String & query = getTableCreationSQL(payload, shard);
    if (query_context.getGlobalContext().isDistributed() && (!params.contains("_sync") || params.at("_sync") != "true"))
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        const String & payload_str = payload_str_stream.str();
        query_context.setPayload(payload_str);
    }

    return processQuery(query);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & /*http_status*/) const
{
    const auto & params = query_context.getQueryParameters();
    if (params.contains("query_id"))
    {
        query_context.setCurrentQueryId(params.at("query_id"));
    }

    if (query_context.getGlobalContext().isDistributed() && (!params.contains("_sync") || params.at("_sync") != "true"))
    {
        query_context.setMutateDistributedMergeTreeTableLocally(false);
    }

    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");
    return processQuery("DROP TABLE " + database_name + "." + table_name);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const auto & params = query_context.getQueryParameters();
    if (params.contains("query_id"))
    {
        query_context.setCurrentQueryId(params.at("query_id"));
    }

    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    LOG_INFO(log, "Updating table {}.{}", database_name, table_name);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    if (query_context.getGlobalContext().isDistributed() && (!params.contains("_sync") || params.at("_sync") != "true"))
    {
        query_context.setMutateDistributedMergeTreeTableLocally(false);

        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        const String & payload_str = payload_str_stream.str();
        query_context.setPayload(payload_str);
    }

    return processQuery(query);
}

String TableRestRouterHandler::buildResponse() const
{
    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

String TableRestRouterHandler::processQuery(const String & query) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    if (io.pipeline.initialized())
    {
        return "TableRestRouterHandler execute io.pipeline.initialized not implemented";
    }

    return buildResponse();
}

String TableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Array::Ptr & columns, const String & time_column) const
{
    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    using std::begin;
    using std::end;
    std::vector<String> column_definitions;

    for (auto & col : *columns)
    {
        column_definitions.push_back(getColumnDefinition(col.extract<Poco::JSON::Object::Ptr>()));
    }

    /// FIXME : we can choose between ALIAS or DEFAULT
    std::copy(begin(column_definitions), end(column_definitions), std::ostream_iterator<String>(oss, ","));
    return oss.str() + " `_time` DateTime64(3) ALIAS " + time_column;
}

String TableRestRouterHandler::getColumnDefinition(const Poco::JSON::Object::Ptr & column) const
{
    std::vector<String> create_segments;

    create_segments.push_back(column->get("name").toString());
    if (column->has("nullable") && column->get("nullable"))
    {
        create_segments.push_back(" Nullable(" + column->get("type").toString() + ")");
    }
    else
    {
        create_segments.push_back(" " + column->get("type").toString());
    }

    if (column->has("default"))
    {
        create_segments.push_back(" DEFAULT " + column->get("default").toString());
    }

    if (column->has("compression_codec"))
    {
        create_segments.push_back(" CODEC(" + column->get("compression_codec").toString() + ")");
    }

    if (column->has("ttl_expression"))
    {
        create_segments.push_back(" TTL " + column->get("ttl_expression").toString());
    }

    if (column->has("skipping_index_expression"))
    {
        create_segments.push_back(", " + column->get("skipping_index_expression").toString());
    }

    return boost::algorithm::join(create_segments, " ");
}

}
