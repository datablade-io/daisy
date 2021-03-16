#include "TableRestRouterHandler.h"

#include <Server/RestRouterHandlers/Common/SchemaValidator.h>
#include <Interpreters/executeQuery.h>

#include <vector>
#include <Core/Block.h>
#include <Poco/Path.h>


namespace DB
{

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
                    {"shards", "int"},
                    {"replication_factor", "int"},
                    {"order_by_expression", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

void TableRestRouterHandler::parseURL(const Poco::Path & path)
{
    database_name = path[DATABASE_DEPTH_INDEX - 1];
    table_name = path[TABLE_DEPTH_INDEX - 1];
}

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload) const
{
    SchemaValidator::validateSchema(create_schema, payload);
    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (auto & col : *columns)
    {
        SchemaValidator::validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>());
    }
    return true;
}

bool TableRestRouterHandler::validateGet(const Poco::JSON::Object::Ptr & payload) const
{
    payload.isNull();
    return true;
}

bool TableRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload) const
{
    SchemaValidator::validateSchema(update_schema, payload);
    return true;
}

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String query = "show databases";
    return processQuery(query, http_status);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String query = "CREATE TABLE " + database_name + "." + payload->get("name").toString() + "("
        + getColumnsDefination(payload->getArray("columns"), payload->get("_time_column").toString())
        + ") ENGINE = MergeTree() PARTITION BY " + payload->get("partition_by_expression").toString() + " ORDER BY "
        + payload->get("order_by_expression").toString() + " TTL " + payload->get("ttl_expression").toString();

    return processQuery(query, http_status);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String query = "DROP TABLE " + database_name + "." + table_name;
    return processQuery(query, http_status);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String query = "ALTER TABLE " + database_name + "." + table_name;
    if (payload->has("order_by_expression") && payload->has("ttl_expression"))
        query = query + " MODIFY" + " ORDER BY " + payload->get("order_by_expression").toString() + ", MODIFY  TTL "
            + payload->get("ttl_expression").toString();
    else if (payload->has("ttl_expression"))
        query = query + " MODIFY  TTL " + payload->get("ttl_expression").toString();
    else
        query = query + " MODIFY" + " ORDER BY " + payload->get("order_by_expression").toString();

    return processQuery(query, http_status);
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
    else
    {
        Poco::JSON::Object resp;
        resp.set("query_id", query_context.getClientInfo().initial_query_id);
        std::stringstream resp_str_stream;
        resp.stringify(resp_str_stream, 4);
        String resp_str = resp_str_stream.str();
        return resp_str;
    }
}

String TableRestRouterHandler::getColumnsDefination(const Poco::JSON::Array::Ptr & columns, const String & time_column) const
{
    std::ostringstream oss;
    using std::begin;
    using std::end;
    std::vector<String> column_definatins;

    for (auto & col : *columns)
    {
        column_definatins.push_back(getColumnDefination(col.extract<Poco::JSON::Object::Ptr>()));
    }

    std::copy(begin(column_definatins), end(column_definatins), std::ostream_iterator<String>(oss, ","));
    return oss.str() + " `_time` DateTime64(3) ALIAS " + time_column;
}

String TableRestRouterHandler::getColumnDefination(const Poco::JSON::Object::Ptr & column) const
{
    String column_def = column->get("name").toString();

    if (column->has("nullable") && column->get("nullable"))
    {
        column_def += " Nullable(" + column->get("type").toString() + ")";
    }
    else
    {
        column_def += " " + column->get("type").toString();
    }

    if (column->has("default"))
    {
        column_def += " DEFAULT " + column->get("default").toString();
    }

    if (column->has("compression_codec"))
    {
        column_def += " CODEC(" + column->get("compression_codec").toString() + ")";
    }

    if (column->has("ttl_expression"))
    {
        column_def += " TTL " + column->get("ttl_expression").toString();
    }

    if (column->has("skipping_index_expression"))
    {
        column_def += ", " + column->get("skipping_index_expression").toString();
    }

    return column_def;
}

}
