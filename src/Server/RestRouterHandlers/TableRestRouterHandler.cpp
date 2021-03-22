#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <Interpreters/executeQuery.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

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

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload) const
{
    validateSchema(create_schema, payload);
    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (auto & col : *columns)
    {
        validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>());
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
    validateSchema(update_schema, payload);
    return true;
}

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String query = "show databases";
    return processQuery(query, http_status);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String database_name = getPathParameter("database");
    std::vector<String> create_segments;

    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back(getColumnsDefination(payload->getArray("columns"), payload->get("_time_column").toString()));
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = MergeTree() PARTITION BY " + payload->get("partition_by_expression").toString());
    create_segments.push_back("ORDER BY " + payload->get("order_by_expression").toString());
    create_segments.push_back(" TTL " + payload->get("ttl_expression").toString());

    String query = boost::algorithm::join(create_segments, " ");

    return processQuery(query, http_status);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String database_name = getPathParameter("database");
    String table_name = getPathParameter("table");

    String query = "DROP TABLE " + database_name + "." + table_name;
    return processQuery(query, http_status);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String database_name = getPathParameter("database");
    String table_name = getPathParameter("table");
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

    String query = boost::algorithm::join(create_segments, " ");

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
