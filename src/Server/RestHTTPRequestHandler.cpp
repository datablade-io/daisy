#include "RestHTTPRequestHandler.h"

#include "RestRouterHandlers/RestRouterHandler.h"
#include "RestRouterHandlers/RestRouterFactory.h"

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryParameterVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/IServer.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/getFQDNOrHostName.h>
#include <ext/scope_guard.h>

#include <iomanip>
#include <Poco/Net/HTTPStream.h>

namespace DB
{

static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
    {
        return HTTPResponse::HTTP_UNAUTHORIZED;
    }
    else if (
        exception_code == ErrorCodes::CANNOT_PARSE_TEXT || exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
        || exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING || exception_code == ErrorCodes::CANNOT_PARSE_DATE
        || exception_code == ErrorCodes::CANNOT_PARSE_DATETIME || exception_code == ErrorCodes::CANNOT_PARSE_NUMBER
        || exception_code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST
        || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE || exception_code == ErrorCodes::TOO_DEEP_AST
        || exception_code == ErrorCodes::TOO_BIG_AST || exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE
        || exception_code == ErrorCodes::SYNTAX_ERROR || exception_code == ErrorCodes::INCORRECT_DATA
        || exception_code == ErrorCodes::TYPE_MISMATCH)
    {
        return HTTPResponse::HTTP_BAD_REQUEST;
    }
    else if (
        exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_FUNCTION
        || exception_code == ErrorCodes::UNKNOWN_IDENTIFIER || exception_code == ErrorCodes::UNKNOWN_TYPE
        || exception_code == ErrorCodes::UNKNOWN_STORAGE || exception_code == ErrorCodes::UNKNOWN_DATABASE
        || exception_code == ErrorCodes::UNKNOWN_SETTING || exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING
        || exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION || exception_code == ErrorCodes::UNKNOWN_FORMAT
        || exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
    {
        return HTTPResponse::HTTP_NOT_FOUND;
    }
    else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
    {
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    }
    else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
    {
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    }
    else if (exception_code == ErrorCodes::SOCKET_TIMEOUT || exception_code == ErrorCodes::CANNOT_OPEN_FILE)
    {
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    }
    else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
    {
        return HTTPResponse::HTTP_LENGTH_REQUIRED;
    }

    return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}


void RestHTTPRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("RestHTTPRequHandler");
    ThreadStatus thread_status;

    /// Should be initialized before anything,
    /// For correct memory accounting.
    Context context = server.context();

    /// Handles
    /// 1) Common HTTP header parsing, user / password etc validation, client_info, setup query_id, query context etc
    /// 2) Common logging
    /// 3) Setup common reponse headers etc

    /// Path validation and extraction logic
    Poco::URI uri(request.getURI());
    Poco::Path path(uri.getPath());
    int pathDepth = path.depth();

    if (pathDepth <= CATEGORY_DEPTH_INDEX)
    {
        throw Exception("The Restful request path length is unreasonable", ErrorCodes::UNACCEPTABLE_URL);
    }

    String route = "";
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        route = path[pathDepth];
    }
    else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE || request.getMethod() == Poco::Net::HTTPRequest::HTTP_PATCH)
    {
        route = path[pathDepth - 1];
    }

    bool with_stacktrace = false;
    try
    {
        /// Setup common reponse headers etc
        response.setContentType("application/json; charset=UTF-8");

        route = "/" + path[PROJECT_DEPTH_INDEX] + "/" + path[VERSION_DEPTH_INDEX] + "/" + path[CATEGORY_DEPTH_INDEX] + "/" + route;
        auto router_handler = RestRouterFactory::instance().get(route, context);

        Int32 http_status = http_status = Poco::Net::HTTPResponse::HTTPResponse::HTTP_OK;
        auto response_payload{router_handler->execute(request, http_status)};

        /// Send back result
        response.setStatusAndReason(exceptionCodeToHTTPStatus(http_status));
        *response.send() << response_payload << std::endl;

        LOG_DEBUG(log, "Done processing query");
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */

        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        int exception_code = getCurrentExceptionCode();

        trySendExceptionToClient(exception_message, exception_code, request, response);
    }
}

RestHTTPRequestHandler::RestHTTPRequestHandler(IServer & server_, const std::string & name) : server(server_), log(&Poco::Logger::get(name))
{
}

void RestHTTPRequestHandler::trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));

        /// FIXME: make sure that no one else is reading from the same stream at the moment.

        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && response.getKeepAlive()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED && !request.getStream().eof())
        {
            request.getStream().ignoreAll();
        }

        bool auth_fail = exception_code == ErrorCodes::UNKNOWN_USER || exception_code == ErrorCodes::WRONG_PASSWORD
            || exception_code == ErrorCodes::REQUIRED_PASSWORD;

        if (auth_fail)
        {
            response.requireAuthentication("ClickHouse server HTTP API");
        }
        else
        {
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
        }

        if (!response.sent())
        {
            /// If nothing was sent yet and we don't even know if we must compress the response.
            *response.send() << s << std::endl;
        }
        else    
        {
            assert(false);
            __builtin_unreachable();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}

}
