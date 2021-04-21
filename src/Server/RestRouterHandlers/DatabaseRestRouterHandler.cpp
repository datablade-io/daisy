#include "DatabaseRestRouterHandler.h"

#include "SchemaValidator.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <DataTypes/DataTypeString.h>
#include <DistributedMetadata/CatalogService.h>
#include <Interpreters/executeQuery.h>

#include <boost/functional/hash.hpp>

namespace DB
{

namespace
{
std::map<String, std::map<String, String>> CREATE_SCHEMA = {
    {"required",{
                    {"name", "string"}
                }
    }
};
}

bool DatabaseRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

String DatabaseRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status */) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & databases = catalog_service.databases();

    Poco::JSON::Array databases_mapping_json;
    for (const auto & database : databases)
    {
        databases_mapping_json.add(database);
    }

    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getCurrentQueryId());
    resp.set("databases", databases_mapping_json);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

String DatabaseRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
        query_context->setDistributedDDLOperation(true);
    }

    const String & database_name = payload->get("name").toString();
    String query = "CREATE DATABASE " + database_name;

    return processQuery(query, http_status);
}

String DatabaseRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context->setDistributedDDLOperation(true);
        query_context->setQueryParameter("_payload", "{}");
    }

    const String & database_name = getPathParameter("database");
    String query = "DROP DATABASE " + database_name;

    return processQuery(query, http_status);
}

String DatabaseRestRouterHandler::processQuery(const String & query, Int32 & /* http_status */) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    if (io.pipeline.initialized())
    {
        return "TableRestRouterHandler execute io.pipeline.initialized not implemented";
    }
    io.onFinish();

    return buildResponse();
}

String DatabaseRestRouterHandler::buildResponse() const
{
    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

}
