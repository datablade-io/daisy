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
    extern const int INCORRECT_DATA;
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
        if (streaming())
        {
            return execute(request.getStream(), response, http_status);
        }
        else
        {
            String data = "{}";

            auto size = request.getContentLength();
            if (size > 0)
            {
                data.resize(size);
                request.getStream().readStrict(data.data(), size);
            }

            Poco::JSON::Parser parser;
            auto payload = parser.parse(data).extract<Poco::JSON::Object::Ptr>();

            if (!validate(request.getMethod(), payload))
            {
                http_status = 500;
                return "Parameter verification failed, please check the parameter information that must be carried";
            }

            return execute(payload, http_status);
        }
    }

    String getUriParamValue(const String & key) const
    {
        String value = "";
        auto iter = query_context.getQueryParameters().find(key);

        if (iter != query_context.getQueryParameters().end())
        {
            value = iter->second;
        }
        else
        {
            throw Exception("Cannot get uri param : " + key + " for rest http handling ", ErrorCodes::INCORRECT_DATA);
        }

        return value;
    }

private:
    virtual bool streaming() { return false; }

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

    virtual String executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
    {
        http_status = 404;
        String result = "DDL DELETE executer not implemented";

        LOG_DEBUG(log, result);
        return result;
    }

    virtual String executePatch(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
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
