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


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

class RestRouterHandler : private boost::noncopyable
{
public:
    RestRouterHandler(ContextPtr query_context_, const String & router_name)
        : query_context(query_context_), log(&Poco::Logger::get(router_name))
    {
    }
    virtual ~RestRouterHandler() = default;

    /// Execute request and return response in `String`. If it failed
    /// a correct `http_status` code will be set by trying best.
    /// This function may throw, and caller will need catch the exception
    /// and sends back HTTP `500` to clients
    void execute(HTTPServerRequest & request, HTTPServerResponse & response);

    const String & getPathParameter(const String & name, const String & default_value = "") const
    {
        auto iter = path_parameters.find(name);
        if (iter != path_parameters.end())
        {
            return iter->second;
        }
        else
        {
            return default_value;
        }
    }

    void setupDistributedQueryParameters(const std::map<String, String> & parameters, const Poco::JSON::Object::Ptr & payload = nullptr) const
    {
        if (!isDistributedDDL())
        {
            return;
        }

        if(payload)
        {
            std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
            payload->stringify(payload_str_stream, 0);
            query_context->setQueryParameter("_payload", payload_str_stream.str());
        }
        else
        {
            /// keep payload consistent with the schema in interper interpreters
            query_context->setQueryParameter("_payload", "{}");
        }

        for (const auto & kv : parameters)
        {
            query_context->setQueryParameter(kv.first, kv.second);
        }
        query_context->setDistributedDDLOperation(true);
    }

    inline bool isDistributedDDL() const
    {
        return query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false";
    }

    void setPathParameter(const String & name, const String & value) { path_parameters[name] = value; }

    const String & getQueryParameter(const String & name, const String & default_value = "") const
    {
        return query_parameters->get(name, default_value);
    }

    const String & getAcceptEncoding() const { return accepted_encoding; }

    Int64 getContentLength() const { return content_length; }

    bool hasQueryParameter(const String & name) const { return query_parameters->has(name); }

    virtual bool streamingOutput() const { return false; }

public:
    static String jsonErrorResponse(const String & error_msg, int error_code, const String & query_id)
    {
        std::stringstream error_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Object error_resp;

        error_resp.set("error_msg", error_msg);
        error_resp.set("code", error_code);
        error_resp.set("request_id", query_id);
        error_resp.stringify(error_str_stream, 0);
        return error_str_stream.str();
    }

protected:
    String jsonErrorResponse(const String & error_msg, int error_code) const
    {
        return jsonErrorResponse(error_msg, error_code, query_context->getCurrentQueryId());
    }

private:
    /// Override this function if derived handler need read data in a streaming way from http input
    virtual bool streamingInput() const { return false; }

    /// Handle the request in streaming way, so far Ingest API probably needs override this function
    virtual String execute(ReadBuffer & /* input */, Int32 & http_status) const { return handleNotImplemented(http_status); }

    /// Sending response in a streaming way
    virtual void execute(const Poco::JSON::Object::Ptr & /* payload */, HTTPServerResponse & response) const
    {
        Int32 http_status = 200;
        auto response_payload{handleNotImplemented(http_status)};
        response.setStatusAndReason(HTTPResponse::HTTPStatus(http_status));
        *response.send() << response_payload << std::endl;
    }

    String handleNotImplemented(Int32 & http_status) const
    {
        http_status = HTTPResponse::HTTP_NOT_IMPLEMENTED;
        return jsonErrorResponse("HTTP method requested is not supported", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
    }

    String execute(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
    {
        const auto & client_info = query_context->getClientInfo();

        if (client_info.http_method == ClientInfo::HTTPMethod::GET)
        {
            return doExecute(&RestRouterHandler::validateGet, &RestRouterHandler::executeGet, payload, http_status);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::POST)
        {
            return doExecute(&RestRouterHandler::validatePost, &RestRouterHandler::executePost, payload, http_status);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::PATCH)
        {
            return doExecute(&RestRouterHandler::validatePatch, &RestRouterHandler::executePatch, payload, http_status);
        }
        else if (client_info.http_method == ClientInfo::HTTPMethod::DELETE)
        {
            return doExecute(&RestRouterHandler::validateDelete, &RestRouterHandler::executeDelete, payload, http_status);
        }

        return handleNotImplemented(http_status);
    }

    template <typename Validate, typename Execute>
    String doExecute(Validate validate, Execute exec, const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
    {
        String error_msg;
        if (!(this->*validate)(payload, error_msg))
        {
            http_status = HTTPResponse::HTTP_BAD_REQUEST;
            return jsonErrorResponse(error_msg, ErrorCodes::BAD_REQUEST_PARAMETER);
        }
        return (this->*exec)(payload, http_status);
    }

    virtual String executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        return handleNotImplemented(http_status);
    }

    virtual String executePost(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        return handleNotImplemented(http_status);
    }

    virtual String executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        return handleNotImplemented(http_status);
    }

    virtual String executePatch(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        return handleNotImplemented(http_status);
    }

    virtual bool validateGet(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validatePost(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validateDelete(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }
    virtual bool validatePatch(const Poco::JSON::Object::Ptr & /* payload */, String & /* error_msg */) const { return true; }

    void setupHTTPContext(const HTTPServerRequest & request)
    {
        accepted_encoding = request.get("Accept-Encoding", "");
        content_length = request.getContentLength64();
        setupQueryParams(request);
    }

    void setupQueryParams(const HTTPServerRequest & request) { query_parameters = std::make_unique<HTMLForm>(request); }

protected:
    ContextPtr query_context;
    Poco::Logger * log;

    std::unordered_map<String, String> path_parameters;
    std::unique_ptr<HTMLForm> query_parameters;
    String accepted_encoding;
    Int64 content_length;
};

using RestRouterHandlerPtr = std::shared_ptr<RestRouterHandler>;

}
