#include "RestStatusHandler.h"

#include <Core/Block.h>
#include <Interpreters/executeSelectQuery.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{
    std::map<String, String> colname_bldkey_mapping = {{"VERSION_DESCRIBE", "version"}, {"BUILD_TIME", "time"}};

    String buildResponse(const Poco::JSON::Object & resp)
    {
        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);

        return resp_str_stream.str();
    }
}

String RestStatusHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & /*http_status*/) const
{
    const String & status = getPathParameter("status");

    if (status == "info")
    {
        String query = "SELECT name, value FROM system.build_options WHERE name IN ('VERSION_FULL','VERSION_DESCRIBE','BUILD_TIME');";

        String resp;
        executeSelectQuery(query, query_context, [this, &resp](Block && block) { return this->buildInfoFromBlock(block, resp); });

        return resp;
    }
    else if (status == "health")
    {
        /// FIXME : introduce more sophisticated health calculation in future.
        return "{\"status\":\"UP\"}";
    }
    else
    {
        return jsonErrorResponse("Unknown URI", ErrorCodes::UNKNOWN_TYPE_OF_QUERY, query_context->getCurrentQueryId());
    }
}

bool RestStatusHandler::validateSchema(const Block & block, const std::vector<String> & col_names) const
{
    for (const auto & col_name : col_names)
    {
        if (!block.has(col_name))
        {
            LOG_ERROR(log, "`{}` column is missing", col_name);
            return false;
        }
    }
    return true;
}

void RestStatusHandler::buildInfoFromBlock(const Block & block, String & resp) const
{
    if (!validateSchema(block, {"name", "value"}))
    {
        resp = jsonErrorResponse("Build column not found ", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, query_context->getCurrentQueryId());
        return;
    }

    const auto & name = block.findByName("name")->column;
    const auto & value = block.findByName("value")->column;

    Poco::JSON::Object build_info;
    for (size_t i = 0; i < name->size(); ++i)
    {
        const auto & it = colname_bldkey_mapping.find(name->getDataAt(i).toString());
        if (it != colname_bldkey_mapping.end())
        {
            build_info.set(it->second, value->getDataAt(i).toString());
        }
    }
    build_info.set("name", "Daisy");

    Poco::JSON::Object resp_json;
    resp_json.set("build", build_info);
    resp = buildResponse(resp_json);
}

}
