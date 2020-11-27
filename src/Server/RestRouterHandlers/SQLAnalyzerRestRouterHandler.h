#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class SQLAnalyzerRestRouterHandler final : public RestRouterHandler
{
public:
    explicit SQLAnalyzerRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "SQLAnalyzer") { }
    ~SQLAnalyzerRestRouterHandler() override { }

    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    static std::map<String, std::map<String, String>> post_schema;

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload) const override;
};

}
