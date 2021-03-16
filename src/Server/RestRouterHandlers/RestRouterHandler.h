#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>
#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>
#include <Poco/Path.h>


#define PROJECT_DEPTH_INDEX 0
#define VERSION_DEPTH_INDEX 1
#define CATEGORY_DEPTH_INDEX 2
#define RAWSTORE_DEPTH_INDEX 3
#define DATABASE_DEPTH_INDEX 4
#define TABLE_DEPTH_INDEX 4
#define COLUMN_DEPTH_INDEX 5

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


class RestRouterHandler : private boost::noncopyable
{
public:
    RestRouterHandler(Context & query_context_, const String & router_name)
        : query_context(query_context_), log(&Poco::Logger::get(router_name))
    {
    }
    virtual ~RestRouterHandler() = default;

    /// Execute request and return response in `String`. If it failed
    /// a correct `http_status` code will be set by trying best.
    /// This function may throw, and caller will need catch the exception
    /// and sends back HTTP `500` to clients
    String execute(HTTPServerRequest & request, HTTPServerResponse & response, Int32 & http_status)
    {
        HTMLForm params(request);
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
                throw Exception(
                    "Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods "
                    "simultaneously",
                    ErrorCodes::REQUIRED_PASSWORD);
            }
        }

        /// Set client info. It will be used for quota accounting parameters in 'setUser' method.
        ClientInfo & client_info = query_context.getClientInfo();
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
        query_context.setUser(user, password, request.clientAddress());
        if (!quota_key.empty())
            query_context.setQuotaKey(quota_key);

        /// Query sent through HTTP interface is initial.
        client_info.initial_user = client_info.current_user;
        client_info.initial_address = client_info.current_address;

        // Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
        query_context.setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));

        client_info.initial_query_id = client_info.current_query_id;

        /// url parse
        Poco::URI uri(request.getURI());
        Poco::Path path(uri.getPath());
        parseURL(path);

        if (streaming())
        {
            return execute(request.getStream(), response, http_status);
        }
        else
        {
            /// Handle over the request.istream to `json` parser directly
            String data = "{}";
            if (request.getMethod() != HTTPServerRequest::HTTP_GET)
            {
                auto size = request.getContentLength();
                data.resize(size);
                request.getStream().readStrict(data.data(), size);
            }

            Poco::JSON::Parser parser;
            auto payload = parser.parse(data).extract<Poco::JSON::Object::Ptr>();

            if (!validate(request.getMethod(), payload))
            {
                throw Exception(
                    "Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods "
                    "simultaneously",
                    ErrorCodes::REQUIRED_PASSWORD);
            }

            return execute(payload, http_status);
        }
    }

private:
    virtual bool streaming() { return false; }
    virtual void parseURL(const Poco::Path & path) = 0;

    /// Streaming `execute`, so far Ingest API probably needs override this function
    virtual String execute(ReadBuffer & /* input */, HTTPServerResponse & /*r esponse */, Int32 & http_status)
    {
        http_status = 404;
        String result = "Streaming executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

private:
    /// Admin APIs like DDL overrides this function
    String execute(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
    {
        if (query_context.getClientInfo().http_method == ClientInfo::HTTPMethod::GET)
        {
            return executeGet(payload, http_status);
        }
        else if (query_context.getClientInfo().http_method == ClientInfo::HTTPMethod::POST)
        {
            return executePost(payload, http_status);
        }
        else if (query_context.getClientInfo().http_method == ClientInfo::HTTPMethod::PATCH)
        {
            return executePatch(payload, http_status);
        }
        else if (query_context.getClientInfo().http_method == ClientInfo::HTTPMethod::DELETE)
        {
            return executeDelete(payload, http_status);
        }
        http_status = 404;

        return "";
    }

    virtual String executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        http_status = 404;
        String result = "DDL GET executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

    virtual String executePost(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        http_status = 404;
        String result = "DDL POST executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

    virtual String executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
    {
        http_status = 404;
        String result = "DDL DELETE executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

    virtual String executePatch(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
    {
        http_status = 404;
        String result = "DDL PATCH executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

private:
    bool validate(const String & http_method, const Poco::JSON::Object::Ptr & payload) const
    {
        if (http_method == Poco::Net::HTTPRequest::HTTP_GET)
        {
            return validateGet(payload);
        }
        else if (http_method == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return validatePost(payload);
        }
        else if (http_method == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return validateDelete(payload);
        }
        else if (http_method == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return validatePatch(payload);
        }

        /// Method we don't support
        return false;
    }

    virtual bool validateGet(const Poco::JSON::Object::Ptr & /* payload */) const { return true; }
    virtual bool validatePost(const Poco::JSON::Object::Ptr & /* payload */) const { return true; }
    virtual bool validateDelete(const Poco::JSON::Object::Ptr & /* payload */) const { return true; }
    virtual bool validatePatch(const Poco::JSON::Object::Ptr & /* payload */) const { return true; }

protected:
    Context & query_context;
    Poco::Logger * log;
};

using RestRouterHandlerPtr = std::shared_ptr<RestRouterHandler>;

}
