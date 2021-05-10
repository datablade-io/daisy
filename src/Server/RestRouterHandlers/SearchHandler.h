#pragma once

#include "RestRouterHandler.h"

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
class SearchHandler final : public RestRouterHandler
{
public:
    explicit SearchHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Search") { }
    ~SearchHandler() override = default;

    String execute(ReadBuffer & input, HTTPServerResponse & response, Int32 & http_status) const override;

    bool outputStreaming() const override { return true; }

private:
    bool streaming() const override { return true; }

    static std::map<String, std::map<String, String>> search_schema;

    static bool validatePayload(const Poco::JSON::Object::Ptr & payload, String & error);

    String getQuery(const Poco::JSON::Object::Ptr & payload) const;

    bool validateQuery(const String & query, String & error) const;

    std::shared_ptr<WriteBufferFromHTTPServerResponse> getOutputBuffer(HTTPServerResponse & response) const;
};
}
