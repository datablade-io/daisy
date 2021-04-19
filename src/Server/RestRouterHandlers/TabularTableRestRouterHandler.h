#pragma once

#include "TableRestRouterHandler.h"

namespace DB
{
class TabularTableRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit TabularTableRestRouterHandler(Context & query_context_) : TableRestRouterHandler(query_context_, "Tabular") { }
    ~TabularTableRestRouterHandler() override { }

private:
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;

    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    const String getDefaultPartitionGranularity() const override;
    const String getDefaultOrderByGranularity() const override;
    const String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const override;
    const String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const override;
};

}
