#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
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
                    {"order_by_expression", "string"},
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

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String query = "show databases";
    return processQuery(query, http_status);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String database_name = getPathParameter("database");
    auto params = query_context.getQueryParameters();

    if (!query_context.getGlobalContext().isDistributed() || (params.contains("_mode") && params["_mode"] == "local"))
    {
        std::vector<String> create_segments;

        create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
        create_segments.push_back("(");
        create_segments.push_back(getColumnsDefination(payload->getArray("columns"), payload->get("_time_column").toString()));
        create_segments.push_back(")");
        create_segments.push_back("ENGINE = MergeTree() PARTITION BY " + payload->get("partition_by_expression").toString());
        create_segments.push_back("ORDER BY " + payload->get("order_by_expression").toString());
        if (payload->has("ttl_expression"))
        {
            create_segments.push_back("TTL " + payload->get("ttl_expression").toString());
        }
        if (payload->has("shard"))
        {
            create_segments.push_back("SETTINGS shard=" + payload->get("shard").toString());
        }

        const String & query = boost::algorithm::join(create_segments, " ");

        return processQuery(query, http_status);
    }
    else
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream);
        const String & payload_str = payload_str_stream.str();

        LOG_INFO(
            log,
            "Creating DistributedMergeTree table {}.{} with payload={}, query_id={}",
            database_name,
            payload->get("name").toString(),
            payload_str,
            query_context.getCurrentQueryId());

        std::vector<std::pair<String, String>> string_cols
            = {std::make_pair("database", database_name),
               std::make_pair("query_id", query_context.getCurrentQueryId()),
               std::make_pair("user", query_context.getUserName()),
               std::make_pair("payload", payload_str)};

        Block block = buildBlock(string_cols);

        IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::CREATE_TABLE, std::move(block)};

        try
        {
            appendRecord(record);
        }
        catch (Exception e)
        {
            LOG_ERROR(
                log,
                "Failed to create DistributedMergeTree table {}.{} with payload={}, query_id={} error={}",
                database_name,
                payload->get("name").toString(),
                payload_str,
                query_context.getCurrentQueryId(),
                e.code());
            throw Exception(e);
        }

        LOG_INFO(
            log,
            "Request of creating DistributedMergeTree table {}.{} with payload={} query_id={} has been accepted",
            database_name,
            payload->get("name").toString(),
            payload_str,
            query_context.getCurrentQueryId());

        return buildResponse();
    }
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    String database_name = getPathParameter("database");
    String table_name = getPathParameter("table");
    auto params = query_context.getQueryParameters();

    if (!query_context.getGlobalContext().isDistributed() || (params.contains("_mode") && params["_mode"] == "local"))
    {
        String query = "DROP TABLE " + database_name + "." + table_name;

        return processQuery(query, http_status);
    }
    else
    {
        LOG_INFO(
            log, "Deleting DistributedMergeTree table {}.{}, query_id={}", database_name, table_name, query_context.getCurrentQueryId());

        std::vector<std::pair<String, String>> string_cols
            = {std::make_pair("database", database_name),
               std::make_pair("table", table_name),
               std::make_pair("query_id", query_context.getCurrentQueryId()),
               std::make_pair("user", query_context.getUserName())};

        Block block = buildBlock(string_cols);
        IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::DELETE_TABLE, std::move(block)};
        try
        {
            appendRecord(record);
        }
        catch (Exception e)
        {
            LOG_ERROR(
                log,
                "Failed to delete DistributedMergeTree table {}.{}, query_id={} error={}",
                database_name,
                table_name,
                query_context.getCurrentQueryId(),
                e.code());
            throw Exception(e);
        }

        LOG_INFO(
            log,
            "Request of delete DistributedMergeTree table {}.{} query_id={} has been accepted",
            database_name,
            table_name,
            query_context.getCurrentQueryId());

        return buildResponse();
    }
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");
    auto params = query_context.getQueryParameters();

    if (!query_context.getGlobalContext().isDistributed() || (params.contains("_mode") && params["_mode"] == "local"))
    {
        bool has_order_by = payload->has("order_by_expression");
        bool has_ttl = payload->has("ttl_expression");

        std::vector<String> create_segments;

        if (has_order_by || has_ttl)
        {
            create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);

            if (payload->has("order_by_expression"))
            {
                create_segments.push_back(" MODIFY ORDER BY " + payload->get("order_by_expression").toString());
            }

            if (has_order_by && has_ttl)
            {
                create_segments.push_back(",");
            }

            if (payload->has("ttl_expression"))
            {
                create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());
            }
        }

        const String & query = boost::algorithm::join(create_segments, " ");

        return processQuery(query, http_status);
    }
    else
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream);
        const String & payload_str = payload_str_stream.str();

        LOG_INFO(
            log,
            "Updating DistributedMergeTree table {}.{} with payload={}, query_id={}",
            database_name,
            table_name,
            payload_str,
            query_context.getCurrentQueryId());

        const std::vector<std::pair<String, String>> string_cols
            = {std::make_pair("database", database_name),
               std::make_pair("table", table_name),
               std::make_pair("query_id", query_context.getCurrentQueryId()),
               std::make_pair("user", query_context.getUserName()),
               std::make_pair("payload", payload_str)};

        Block block = buildBlock(string_cols);
        IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ALTER_TABLE, std::move(block)};

        try
        {
            appendRecord(record);
        }
        catch (Exception e)
        {
            LOG_ERROR(
                log,
                "Failed to update DistributedMergeTree table {}.{} with payload={}, query_id={} error={}",
                database_name,
                table_name,
                payload_str,
                query_context.getCurrentQueryId(),
                e.code());
            throw Exception(e);
        }

        LOG_INFO(
            log,
            "Request of update DistributedMergeTree table {}.{} with payload={} query_id={} has been accepted",
            database_name,
            table_name,
            payload_str,
            query_context.getCurrentQueryId());

        return buildResponse();
    }
}

Block TableRestRouterHandler::buildBlock(const std::vector<std::pair<String, String>> & string_cols) const
{
    Block block;
    auto string_type = std::make_shared<DataTypeString>();

    for (const auto & p : string_cols)
    {
        auto col = string_type->createColumn();
        col->insertData(p.second.data(), p.second.size());
        ColumnWithTypeAndName col_with_type(std::move(col), string_type, p.first);
        block.insert(col_with_type);
    }

    return block;
}

void TableRestRouterHandler::appendRecord(IDistributedWriteAheadLog::Record & record) const
{
    auto wal = DistributedWriteAheadLogPool::instance(query_context.getGlobalContext()).getDefault();
    if (!wal)
    {
        LOG_ERROR(
            log,
            "Distributed environment is not setup. Unable to operate with DistributedMergeTree engine. query_id={} ",
            query_context.getCurrentQueryId());
        throw Exception(
            "Distributed environment is not setup. Unable to operate with DistributedMergeTree engine", ErrorCodes::CONFIG_ERROR);
    }

    std::any ctx{DistributedWriteAheadLogKafkaContext{topic}};

    auto result = wal->append(record, ctx);
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(
            log,
            "Failed to append record to DistributedWriteAheadLog, query_id={}, error={}",
            query_context.getCurrentQueryId(),
            result.err);
        throw Exception("Failed to append record to DistributedWriteAheadLog, error={}", result.err);
    }
}

String TableRestRouterHandler::buildResponse() const
{
    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getClientInfo().initial_query_id);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 4);

    return resp_str_stream.str();
}

String TableRestRouterHandler::processQuery(const String & query, Int32 & /* http_status */) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    if (io.pipeline.initialized())
    {
        return "TableRestRouterHandler execute io.pipeline.initialized not implemented";
    }
    else if (io.in)
    {
        Block block = io.getInputStream()->read();
        return block.getColumns().at(0)->getDataAt(0).data;
    }

    return buildResponse();
}

String TableRestRouterHandler::getColumnsDefination(const Poco::JSON::Array::Ptr & columns, const String & time_column) const
{
    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    using std::begin;
    using std::end;
    std::vector<String> column_definatins;

    for (auto & col : *columns)
    {
        column_definatins.push_back(getColumnDefination(col.extract<Poco::JSON::Object::Ptr>()));
    }

    /// FIXME : we can choose between ALIAS or DEFAULT
    std::copy(begin(column_definatins), end(column_definatins), std::ostream_iterator<String>(oss, ","));
    return oss.str() + " `_time` DateTime64(3) ALIAS " + time_column;
}

String TableRestRouterHandler::getColumnDefination(const Poco::JSON::Object::Ptr & column) const
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
