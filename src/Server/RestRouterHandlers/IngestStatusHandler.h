#pragma once

#include "RestRouterHandler.h"

#include <Storages/StorageDistributedMergeTree.h>


namespace DB
{
using TablePollIdMap = std::unordered_map<std::pair<String, String>, std::vector<String>, boost::hash<std::pair<String, String>>>;

class IngestStatusHandler final : public RestRouterHandler
{
public:
    explicit IngestStatusHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "IngestStatus") { }
    ~IngestStatusHandler() override = default;

    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    static std::map<String, std::map<String, String>> poll_schema;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool streaming() const override { return false; }

    /// forward poll request
    String forwardRequest(const Poco::URI & uri, const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const;
    const IStorage * getAndVerifyStorage(const String & database_name, const String & table_name, String & error, int & error_code) const;
    static String makeBatchResponse(const std::vector<IngestingBlocks::IngestStatus> & statuses);
    bool categorizePollIds(const std::vector<String> & poll_ids, TablePollIdMap & table_poll_ids, String & error) const;
};

}
