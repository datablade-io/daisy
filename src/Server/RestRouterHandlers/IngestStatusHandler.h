#pragma once

#include "RestRouterHandler.h"

#include <Storages/StorageDistributedMergeTree.h>


namespace DB
{
using TableQueries = std::unordered_map<String, std::vector<String>>;

class IngestStatusHandler final : public RestRouterHandler
{
public:
    explicit IngestStatusHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "IngestStatus") { }
    ~IngestStatusHandler() override = default;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    bool streaming() const override { return false; }

    /// forward poll request
    String forwardRequest(const Poco::URI & uri, const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const;
    const StorageDistributedMergeTree *
    getAndVerifyStorage(const String & database_name, const String & table_name, String & error, int & error_code) const;
    static String makeResponse(const std::pair<String, Int32> & status);
    static String makeBatchResponse(const std::vector<std::tuple<String, String, Int32>> & statuses);
    bool parsePollIds(const std::vector<String> & poll_ids, TableQueries & queries, String & error) const;
};

}
