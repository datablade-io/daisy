#include "RestStatusHandler.h"
#include "IServer.h"

#include "RestRouterHandlers/RestRouterHandler.h"

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Poco/Path.h>

#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

void RestStatusHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    if (!request_context)
    {
        // Context should be initialized before anything, for correct memory accounting.
        request_context = Context::createCopy(server.context());
    }

    HTMLForm params(request);
    LOG_TRACE(log, "Request uri: {}", request.getURI());

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    request_context->setCurrentQueryId(params.get("query_id",""));

    /// Setup common response headers etc
    response.setContentType("application/json; charset=UTF-8");

    Poco::URI uri(request.getURI());
    const String & path = uri.getPath();
    const auto & func = uri_funcs.find(path);

    if (func != uri_funcs.end())
    {
        *response.send() << func->second() << std::endl;
    }
    else
    {
        response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
        const auto & resp
            = RestRouterHandler::jsonErrorResponse("Unknown URI", ErrorCodes::UNKNOWN_TYPE_OF_QUERY, request_context->getCurrentQueryId());
        *response.send() << resp << std::endl;
        return;
    }
}

String RestStatusHandler::getInfo()
{
    String query = "SELECT name, value FROM system.build_options WHERE name IN ('VERSION_FULL','VERSION_DESCRIBE','BUILD_TIME');";

    Poco::JSON::Object resp;
    executeSelectQuery(query, request_context, [this, &resp](Block && block) { return this->buildInfoFromBlock(block, resp); });

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
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

void RestStatusHandler::buildInfoFromBlock(const Block & block, Poco::JSON::Object & resp) const
{
    Poco::JSON::Object build_info;
    if (!validateSchema(block, {"name", "value"}))
    {
        return;
    }

    const auto & name = block.findByName("name")->column;
    const auto & value = block.findByName("value")->column;

    for (size_t i = 0; i < name->size(); ++i)
    {
        const String & col_name = name->getDataAt(i).toString();
        const String & col_value = value->getDataAt(i).toString();

        if (col_name == "VERSION_FULL")
        {
            Strings version_fulls;
            boost::split(version_fulls, col_value, boost::is_any_of(" "));

            build_info.set("name", version_fulls[0]);
        }
        else if (col_name == "VERSION_DESCRIBE")
        {
            build_info.set("version", col_value);
        }
        else if (col_name == "BUILD_TIME")
        {
            build_info.set("time", col_value);
        }
        else
        {
            assert(0);
            LOG_ERROR(log, "Unknown name={}", col_name);
        }
    }
    resp.set("build", build_info);
}

void RestStatusHandler::registerFuncs(const String & uri, std::function<String()> callback)
{
    uri_funcs.emplace(uri, callback);
}

RestStatusHandler::RestStatusHandler(IServer & server_, const String & name) : server(server_), log(&Poco::Logger::get(name))
{
    registerFuncs("/daisy/api/v1/info", [this]() { return getInfo(); });
    registerFuncs("/daisy/api/v1/health", [this]() { return getHealth(); });
}

}
