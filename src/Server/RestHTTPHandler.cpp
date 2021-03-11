#include "RestHTTPHandler.h"

#include "RestAction/DDL/DDLTablesAction.h"
#include "HTTPHandlerRequestFilter.h"

#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <Disks/StoragePolicy.h>
#include <IO/ConcatReadBuffer.h>
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
std::map<std::string, std::function<IRestAction *()>> RestActionFactory::dyn_acthion_map;

// Register DAE Achtion
REGISTER_IREATACTION("ddl/tables", DDLTablesAction);


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


RestHTTPHandler::RestHTTPHandler(IServer & server_, const std::string & name) : server(server_), log(&Poco::Logger::get(name))
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


void RestHTTPHandler::trySendExceptionToClient(
    const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output)
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

        if (!response.sent() && !used_output.out_maybe_compressed)
        {
            /// If nothing was sent yet and we don't even know if we must compress the response.
            *response.send() << s << std::endl;
        }
        else if (used_output.out_maybe_compressed)
        {
            /// Destroy CascadeBuffer to actualize buffers' positions and reset extra references
            if (used_output.hasDelayed())
                used_output.out_maybe_delayed_and_compressed.reset();

            /// Send the error message into already used (and possibly compressed) stream.
            /// Note that the error message will possibly be sent after some data.
            /// Also HTTP code 200 could have already been sent.

            /// If buffer has data, and that data wasn't sent yet, then no need to send that data
            bool data_sent = used_output.out->count() != used_output.out->offset();

            if (!data_sent)
            {
                used_output.out_maybe_compressed->position() = used_output.out_maybe_compressed->buffer().begin();
                used_output.out->position() = used_output.out->buffer().begin();
            }

            writeString(s, *used_output.out_maybe_compressed);
            writeChar('\n', *used_output.out_maybe_compressed);

            used_output.out_maybe_compressed->next();
            used_output.out->finalize();
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


void RestHTTPHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("RestHTTPHandler");
    ThreadStatus thread_status;

    /// Should be initialized before anything,
    /// For correct memory accounting.
    Context context = server.context();
    /// Cannot be set here, since query_id is unknown.
    std::optional<CurrentThread::QueryScope> query_scope;

    Output used_output;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    try
    {
        response.setContentType("text/plain; charset=UTF-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);
        /// For keep-alive to work.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
        {
            throw Exception(
                "The Transfer-Encoding is not chunked and there is no Content-Length header for POST request",
                ErrorCodes::HTTP_LENGTH_REQUIRED);
        }

        executeAction(server, log, context, request, params, response, used_output, query_scope);
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

        trySendExceptionToClient(exception_message, exception_code, request, response, used_output);
    }

    if (used_output.out)
        used_output.out->finalize();
}

void RestHTTPHandler::executeAction(
    IServer & server_,
    Poco::Logger * log_,
    Context & context,
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response,
    Output & used_output,
    std::optional<CurrentThread::QueryScope> & query_scope)
{
    Poco::URI uri(request.getURI());
    Poco::Path path(uri.getPath());
    int pathDepth = path.depth();

    String route = "";
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        route = path[pathDepth];
    }
    else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE || request.getMethod() == Poco::Net::HTTPRequest::HTTP_PATCH)
    {
        route = path[pathDepth - 1];
    }

    String api_category = path[CATEGORY_DEPTH - 1]; // DDL、INGEST
    IRestAction * obj = RestActionFactory::produce(api_category + "/" + route);

    if (obj == nullptr)
    {
        throw Exception("Invalid path name " + uri.getPath() + " for DAE HTTPHandler. ", ErrorCodes::UNKNOWN_FUNCTION);
    }
    else
    {
        obj->execute(server_, log_, context, request, params, response, used_output, query_scope, path);
    }
}

}
