#pragma once

#include "RestRouterHandler.h"

#include <Processors/QueryPipeline.h>

namespace DB
{
class DatabaseRestRouterHandler final : public RestRouterHandler
{
public:
    explicit DatabaseRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Database") { }
    ~DatabaseRestRouterHandler() override { }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;

private:
    std::pair<String, Int32> processQuery(const String & query) const;
    String buildResponse() const;
    void processQueryWithProcessors(Poco::JSON::Object & resp, QueryPipeline & pipeline) const;
};

}
