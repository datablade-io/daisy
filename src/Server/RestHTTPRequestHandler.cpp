#include "RestHTTPRequestHandler.h"

#include "RestRouterHandlers/RestRouterFactory.h"
#include "RestRouterHandlers/RestRouterHandler.h"

#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <Common/setThreadName.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_PARSE_TEXT;
extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
extern const int CANNOT_PARSE_QUOTED_STRING;
extern const int CANNOT_PARSE_DATE;
extern const int CANNOT_PARSE_DATETIME;
extern const int CANNOT_PARSE_NUMBER;
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_COMPILE_REGEXP;

extern const int UNKNOWN_ELEMENT_IN_AST;
extern const int UNKNOWN_TYPE_OF_AST_NODE;
extern const int TOO_DEEP_AST;
extern const int TOO_BIG_AST;
extern const int UNEXPECTED_AST_STRUCTURE;

extern const int SYNTAX_ERROR;

extern const int INCORRECT_DATA;
extern const int TYPE_MISMATCH;

extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_IDENTIFIER;
extern const int UNKNOWN_TYPE;
extern const int UNKNOWN_STORAGE;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_SETTING;
extern const int UNKNOWN_DIRECTION_OF_SORTING;
extern const int UNKNOWN_AGGREGATE_FUNCTION;
extern const int UNKNOWN_FORMAT;
extern const int UNKNOWN_DATABASE_ENGINE;
extern const int UNKNOWN_TYPE_OF_QUERY;
extern const int NO_ELEMENTS_IN_CONFIG;

extern const int QUERY_IS_TOO_LARGE;

extern const int NOT_IMPLEMENTED;
extern const int SOCKET_TIMEOUT;

extern const int UNKNOWN_USER;
extern const int WRONG_PASSWORD;
extern const int REQUIRED_PASSWORD;

extern const int BAD_REQUEST_PARAMETER;
extern const int INVALID_SESSION_TIMEOUT;
extern const int HTTP_LENGTH_REQUIRED;
extern const int UNACCEPTABLE_URL;
}

namespace
{
Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
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
}


void RestHTTPRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("RestHandler");
    ThreadStatus thread_status;

    /// Should be initialized before anything,
    /// For correct memory accounting.
    Context context = server.context();

    HTMLForm params(request);
    LOG_TRACE(log, "Request Router: {}", request.getURI());

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    String user = request.get("X-ClickHouse-User", "");
    String password = request.get("X-ClickHouse-Key", "");
    String quota_key = request.get("X-ClickHouse-Quota", "");

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            Poco::Net::HTTPBasicCredentials credentials(request);

            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }
        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials() || params.has("user") || params.has("password") || params.has("quota_key"))
        {
            trySendExceptionToClient(
                "Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods "
                "simultaneously",
                (ErrorCodes::REQUIRED_PASSWORD),
                request,
                response);
            return ;
        }
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.
    ClientInfo & client_info = context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::HTTP;

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;
    else if (request.getMethod() == HTTPServerRequest::HTTP_PATCH)
        http_method = ClientInfo::HTTPMethod::PATCH;
    else if (request.getMethod() == HTTPServerRequest::HTTP_DELETE)
        http_method = ClientInfo::HTTPMethod::DELETE;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");

    /// This will also set client_info.current_user and current_address
    context.setUser(user, password, request.clientAddress());
    if (!quota_key.empty())
        context.setQuotaKey(quota_key);

    /// Query sent through HTTP interface is initial.
    client_info.initial_user = client_info.current_user;
    client_info.initial_address = client_info.current_address;

    // Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context.setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));
    client_info.initial_query_id = client_info.current_query_id;

    try
    {
        auto router_handler = RestRouterFactory::instance().get(request.getURI(), request.getMethod(), context);
        if (router_handler == nullptr)
        {
            trySendExceptionToClient(
                "Cannot find the handler corresponding to the route :  " + request.getURI() + " and the request method  is : " + request.getMethod(),
                (ErrorCodes::UNKNOWN_FUNCTION),
                request,
                response);
            return ;
        }

        /// Setup common reponse headers etc
        response.setContentType("application/json; charset=UTF-8");
        Int32 http_status = HTTPResponse::HTTP_OK;

        LOG_DEBUG(log, "Start processing query_id={} user={}", client_info.current_query_id, user);
        auto response_payload{router_handler->execute(request, response, http_status)};

        /// Send back result
        response.setStatusAndReason(HTTPResponse::HTTPStatus(http_status));
        *response.send() << response_payload << std::endl;

        LOG_DEBUG(log, "End of processing query_id={} user={}", client_info.current_query_id, user);
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        int exception_code = getCurrentExceptionCode();

        std::stringstream error_str_stream;
        Poco::JSON::Object error_resp;

        error_resp.set("error_messgae", getCurrentExceptionMessage(false, true));
        error_resp.set("request_id", client_info.current_query_id);
        error_resp.stringify(error_str_stream, 4);

        std::string exception_message = error_str_stream.str();
        trySendExceptionToClient(exception_message, exception_code, request, response);
    }
}

RestHTTPRequestHandler::RestHTTPRequestHandler(IServer & server_, const std::string & name) : server(server_), log(&Poco::Logger::get(name))
{
}

void RestHTTPRequestHandler::trySendExceptionToClient(
    const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
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
