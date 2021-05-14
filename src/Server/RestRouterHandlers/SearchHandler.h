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

    void execute(const Poco::JSON::Object::Ptr & payload, HTTPServerResponse & response) const override;

    bool streamingOutput() const override { return true; }

private:
    bool streamingInput() const override { return false; }

    static bool validatePayload(const Poco::JSON::Object::Ptr & payload, String & error);

    String getQuery(const Poco::JSON::Object::Ptr & payload) const;

    bool validateQuery(const String & query, String & error) const;

    std::shared_ptr<WriteBufferFromHTTPServerResponse> getOutputBuffer(HTTPServerResponse & response) const;
};
}
