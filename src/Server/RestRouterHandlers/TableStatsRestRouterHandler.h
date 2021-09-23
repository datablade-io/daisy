#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class TableStatsRestRouterHandler final : public RestRouterHandler
{
public:
    explicit TableStatsRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "TableStats") { }
    ~TableStatsRestRouterHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;

    int buildLocalTableStatsJSON(Poco::JSON::Object & resp, const String & table) const;

    using TableResponse = std::tuple<String, Int32, String, Int32>;
    using TableResponses = std::vector<TableResponse>;
    String mergeLocalTableStatsToString(const TableResponses & table_responses) const;
};
}
