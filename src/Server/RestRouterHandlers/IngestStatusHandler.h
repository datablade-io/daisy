#pragma once

#include "RestRouterHandler.h"


namespace DB
{
class IngestStatusHandler final : public RestRouterHandler
{
public:
    explicit IngestStatusHandler(Context & query_context_) : RestRouterHandler(query_context_, "IngestStatus") { }
    ~IngestStatusHandler() override = default;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    bool streaming() const override { return false; }

    /// send http request
    String forwardRequest(const Poco::URI & uri, Int32 & http_status) const;

    static String makeResponse(const std::pair<String, Int32> & status);
};

}
