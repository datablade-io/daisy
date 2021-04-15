#pragma once

#include "TableRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>

namespace DB
{
class RawstoreTableRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit RawstoreTableRestRouterHandler(Context & query_context_) : TableRestRouterHandler(query_context_, "RawStore")
    {
        query_context.setQueryParameter("table_type", "rawstore");
    }
    ~RawstoreTableRestRouterHandler() override { }

private:
    static std::map<String, std::map<String, String>> create_schema;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const override;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload) const;
};

}
