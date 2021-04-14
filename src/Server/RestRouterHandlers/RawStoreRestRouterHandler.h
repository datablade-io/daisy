#pragma once

#include "TableRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>

namespace DB
{
namespace
{
std::map<String, std::map<String, String> > RAWSTORE_CREATE_SCHEMA = {
    {"required",{
                    {"name","string"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {"replication_factor", "int"},
                    {"order_by_granularity", "string"},
                    {"partition_by_granularity", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > RAWSTORE_UPDATE_SCHEMA = {
    {"required",{
                }
    },
    {"optional", {
                    {"ttl_expression", "string"}
                }
    }
};
}

class RawStoreRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit RawStoreRestRouterHandler(Context & query_context_) : TableRestRouterHandler(query_context_, "RawStore"){
        create_schema = RAWSTORE_CREATE_SCHEMA;
        update_schema = RAWSTORE_UPDATE_SCHEMA;
        query_context.setQueryParameter("table_type", "rawstore");
    }
    ~RawStoreRestRouterHandler() override { }
private:
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const override;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload) const;

    void buildTablesJson(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const;
    void buildColumnsJson(Poco::JSON::Object & resp_table, const String & create_table_info) const;

};

}
