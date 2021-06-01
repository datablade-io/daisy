#include "DatabaseRestRouterHandler.h"

#include "SchemaValidator.h"

#include <Core/Block.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

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

std::pair<String, Int32> DatabaseRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    String query = "SHOW DATABASES;";

    Poco::JSON::Object resp;
    return {processQuery(query, ([this, &resp](Block && block) { this->buildDatabaseArray(block, resp); }), resp), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> DatabaseRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    const String & database_name = payload->get("name").toString();
    String query = "CREATE DATABASE " + database_name;

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> DatabaseRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({});
    }

    const String & database_name = getPathParameter("database");
    String query = "DROP DATABASE " + database_name;

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

void DatabaseRestRouterHandler::buildDatabaseArray(const Block & block, Poco::JSON::Object & resp) const
{
    Poco::JSON::Array databases_mapping_json;
    if (block)
    {
        for (size_t index = 0; index < block.rows(); index++)
        {
            const auto & databases_info = block.getColumns().at(0)->getDataAt(index).data;
            databases_mapping_json.add(databases_info);
        }
    }

    resp.set("databases", databases_mapping_json);
}

}

