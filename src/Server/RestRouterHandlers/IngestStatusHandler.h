#pragma once

#include "RestRouterHandler.h"

namespace DB
{

class IngestStatusHandler final : public RestRouterHandler
{
public:
    explicit IngestStatusHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table") {}
    ~IngestStatusHandler() override = default;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    bool streaming() override { return false; }
    void prepare();

    /// send http request
    String forwardRequest(const Poco::URI & uri, Int32 & http_status) const;

    static String makeResponse(const std::pair<String, Int32> & progress);
};

}
