#pragma once

#include "TableRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>

namespace DB
{
class RawStoreRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit RawStoreRestRouterHandler(Context & query_context_);
    ~RawStoreRestRouterHandler() override { }

private:
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const override;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload) const;
};

}
