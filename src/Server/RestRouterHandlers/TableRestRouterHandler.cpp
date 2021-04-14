#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Interpreters/executeQuery.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
namespace
{
std::map<String, std::map<String, String> > CREATE_SCHEMA = {
    {"required",{
                    {"name","string"},
                    {"columns", "array"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {"_time_column", "string"},
                    {"replication_factor", "int"},
                    {"order_by_expression", "string"},
                    {"order_by_granularity", "string"},
                    {"partition_by_granularity", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > COLUMN_SCHEMA = {
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

std::map<String, std::map<String, String> > UPDATE_SCHEMA = {
    {"required",{
                }
    },
    {"optional", {
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, String> GRANULARITY_FUNC_MAPPING= {
    {"M", "toYYYYMM(`_time`)"},
    {"D", "toYYYYMMDD(`_time`)"},
    {"H", "toStartOfHour(`_time`)"},
    {"m", "toStartOfMinute(`_time`)"}
};
}

namespace ErrorCodes
{
    extern const int CONFIG_ERROR;
    extern const int OK;
}

TableRestRouterHandler::TableRestRouterHandler(Context & query_context_, const String & router_name)
    : RestRouterHandler(query_context_, router_name)
    , create_schema(CREATE_SCHEMA)
    , column_schema(COLUMN_SCHEMA)
    , update_schema(UPDATE_SCHEMA)
    , granularity_func_mapping(GRANULARITY_FUNC_MAPPING)
{
}

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
    {
        return false;
    }

    if (payload->has("columns"))
    {
        Poco::JSON::Array::Ptr columns = payload->getArray("columns");
        for (const auto & col : *columns)
        {
            if (!validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>(), error_msg))
            {
                return false;
            }
        }
    }

    if (payload->has("partition_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("partition_by_granularity").toString()))
        {
            error_msg = "Invalid partition_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    if (payload->has("order_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("order_by_granularity").toString()))
        {
            error_msg = "Invalid order_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    /// For non-distributed env or user force to create a `local` MergeTree table
    if (!query_context.isDistributed() || getQueryParameter("distributed") == "false")
    {
        int shards = payload->has("shards") ? payload->get("shards").convert<Int32>() : 1;
        int replication_factor = payload->has("replication_factor") ? payload->get("replication_factor").convert<Int32>() : 1;

        if (shards != 1 || replication_factor != 1)
        {
            error_msg = "Invalid shards / replication factor, local table shall have only 1 shard and 1 replica";
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

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status*/) const
{
    String query = "show databases";
    return processQuery(query);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const auto & shard = getQueryParameter("shard");
    const auto & query = getCreationSQL(payload, shard);

    if (query_context.isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context.setQueryParameter("_payload", payload_str_stream.str());
        query_context.setDistributedDDLOperation(true);
    }

    return processQuery(query);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & /*http_status*/) const
{
    if (query_context.isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context.setDistributedDDLOperation(true);
        query_context.setQueryParameter("_payload", "{}");
    }

    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");
    return processQuery("DROP TABLE " + database_name + "." + table_name);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    LOG_INFO(log, "Updating table {}.{}", database_name, table_name);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    if (query_context.isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context.setDistributedDDLOperation(true);

        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context.setQueryParameter("_payload", payload_str_stream.str());
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
    io.onFinish();

    return buildResponse();
}

String TableRestRouterHandler::getEngineExpr(const Poco::JSON::Object::Ptr & payload) const
{
    if (query_context.isDistributed())
    {
        if (getQueryParameter("distributed") != "false")
        {
            return fmt::format(
                "DistributedMergeTree({}, {}, {})",
                payload->get("replication_factor").toString(),
                payload->get("shards").toString(),
                payload->get("shard_by_expression").toString());
        }
    }

    return "MergeTree()";
}

inline String TableRestRouterHandler::getTimeColumn(const Poco::JSON::Object::Ptr & payload) const
{
    return payload->has("_time_column") ? payload->get("_time_column").toString() : "_time";
}

String TableRestRouterHandler::getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity) const
{
    const auto & partition_by_granularity
        = payload->has("partition_by_granularity") ? payload->get("partition_by_granularity").toString() : default_granularity;
    return granularity_func_mapping[partition_by_granularity];
}

String TableRestRouterHandler::getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/) const
{
    const auto & order_by_granularity = payload->has("order_by_granularity") ? payload->get("order_by_granularity").toString() : "D";
    const auto & default_order_expr = granularity_func_mapping[order_by_granularity];
    const auto & order_by_expression = payload->has("order_by_expression") ? payload->get("order_by_expression").toString() : String();

    if (order_by_expression.empty())
    {
        return default_order_expr;
    }

    /// FIXME: We may need to check whether the time column is already set as the first column in order by expression.

    return default_order_expr + ", " + order_by_expression;
}

String TableRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const
{
    const auto & database_name = getPathParameter("database");
    const auto & time_col = getTimeColumn(payload);
    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back(getColumnsDefinition(payload));
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = " + getEngineExpr(payload));
    create_segments.push_back("PARTITION BY " + getPartitionExpr(payload, "M"));
    create_segments.push_back("ORDER BY (" + getOrderbyExpr(payload, time_col) + ")");

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

String TableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & columns = payload->getArray("columns");

    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    using std::begin;
    using std::end;
    std::vector<String> column_definitions;

    for (const auto & col : *columns)
    {
        column_definitions.push_back(getColumnDefinition(col.extract<Poco::JSON::Object::Ptr>()));
    }

    std::copy(begin(column_definitions), end(column_definitions), std::ostream_iterator<String>(oss, ","));
    if (payload->has("_time_column"))
    {
        return oss.str() + " `_time` DateTime64(3) DEFAULT " + payload->get("_time_column").toString();
    }
    return oss.str() + " `_time` DateTime64(3, UTC) DEFAULT now64(3)";
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
