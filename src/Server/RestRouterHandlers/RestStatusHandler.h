#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class RestStatusHandler final : public RestRouterHandler
{
public:
    explicit RestStatusHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Status") { }
    ~RestStatusHandler() override { }

private:
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;
    void buildInfoFromBlock(const Block & block, String & str) const;
};

}
