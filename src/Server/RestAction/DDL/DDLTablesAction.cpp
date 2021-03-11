#include "DDLTablesAction.h"
#include "Server/RestAction/Common/SchemaValidator.h"
#include <vector>
#include <Core/BaseSettings.h>
#include <Poco/Path.h>

namespace DB
{
std::map<String, std::map<String, String> > DDLTablesAction::create_schema = {
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

std::map<String, std::map<String, String> > DDLTablesAction::column_schema = {
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

std::map<String, std::map<String, String> > DDLTablesAction::update_schema = {
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


String
DDLTablesAction::postParamsJoin(HTTPServerRequest & request, const Poco::Path & path)
{
    std::string data;
    auto size = request.getContentLength();
    data.resize(size);
    request.getStream().readStrict(data.data(), size);

    Poco::JSON::Parser parser;
    Poco::JSON::Object::Ptr payload = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
    validateCreateSchema(payload);
    return getCreateQuery(payload, path[DATABASES_DEPTH - 1]);
}

void DDLTablesAction::validateCreateSchema(Poco::JSON::Object::Ptr payload)
{
    SchemaValidator::validateSchema(create_schema, payload);
    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (auto & col: *columns)
    {
        SchemaValidator::validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>());
    }
}

String DDLTablesAction::getCreateQuery(Poco::JSON::Object::Ptr payload, String db)
{
    String query = "CREATE TABLE " + db + "." + payload->get("name").toString() + "("
        + getColumnsDefination(payload->getArray("columns"), payload->get("_time_column").toString())
        + ") ENGINE = MergeTree() PARTITION BY " + payload->get("partition_by_expression").toString() + " ORDER BY "
        + payload->get("order_by_expression").toString() + " TTL " + payload->get("ttl_expression").toString();
    return query;
}


String DDLTablesAction::getColumnsDefination(Poco::JSON::Array::Ptr columns, String time_column)
{
    std::ostringstream oss;
    using std::begin;
    using std::end;
    std::vector<String> column_definatins;

    for(auto & col: *columns)
    {
        column_definatins.push_back(getColumnDefination(col.extract<Poco::JSON::Object::Ptr>()));
    }

    std::copy(begin(column_definatins), end(column_definatins), std::ostream_iterator<String>(oss, ","));
    return oss.str() + " `_time` DateTime64(3) ALIAS " + time_column;
}

String DDLTablesAction::getColumnDefination(Poco::JSON::Object::Ptr column)
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

std::string DDLTablesAction::patchParamsJoin(HTTPServerRequest &request, const Poco::Path & path)
{
    path[0];
    request.getURI();

    std::string data;
    auto size = request.getContentLength();
    data.resize(size);
    request.getStream().readStrict(data.data(), size);

    Poco::JSON::Parser parser;
    Poco::JSON::Object::Ptr payload = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
    return getUpdateQuery(payload, path[DATABASES_DEPTH - 1], path[path.depth()]);
}


String DDLTablesAction::getUpdateQuery(Poco::JSON::Object::Ptr payload, String db, String table)
{   
    SchemaValidator::validateSchema(update_schema, payload);

    String query = "ALTER TABLE " + db + "." + table;
    if(payload->has("order_by_expression") && payload->has("ttl_expression"))
        query = query + " MODIFY" + " ORDER BY " + payload->get("order_by_expression").toString() + ", MODIFY  TTL " + payload->get("ttl_expression").toString();
    else if(payload->has("ttl_expression"))
        query = query +  " MODIFY  TTL " + payload->get("ttl_expression").toString();
    else if(payload->has("order_by_expression"))
        query = query + " MODIFY" + " ORDER BY " + payload->get("order_by_expression").toString();
    else
        return "";
    return query;
}

String DDLTablesAction::getParamsJoin(HTTPServerRequest & request, const Poco::Path & path)
{
    path[0];
    request.getURI();
    std::cout << "getParamsJoin" << std::endl;
    return "select * from t1;";
}

std::string DDLTablesAction::deleteParamsJoin(const Poco::Path & path)
{

    String query = "DROP TABLE " + path[path.depth() -2] + "." + path[path.depth()];
    return query;
}

void DDLTablesAction::execute(
        IServer & server,
        Poco::Logger * log,      
        Context & context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        Output & used_output,
        std::optional<CurrentThread::QueryScope> & query_scope,
        const Poco::Path & path)
{

    String query = "";
    if(request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
    {
         query = getParamsJoin(request, path);
    }
    else if(request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        query = postParamsJoin(request, path);
    }
    else if(request.getMethod() == Poco::Net::HTTPRequest::HTTP_PATCH)
    {
        query = patchParamsJoin(request, path);
    }
    else if(request.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE)
    {
        query = deleteParamsJoin(path);

    }
    else
    {
        query = "";
    }

    executeByQuery(server, log, context, request, params, response, used_output, query_scope, query);
}

}
