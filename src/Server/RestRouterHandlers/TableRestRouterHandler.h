#pragma once

#include "RestRouterHandler.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Processors/QueryPipeline.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <boost/functional/hash.hpp>

namespace DB
{
class TableRestRouterHandler : public RestRouterHandler
{
public:
    explicit TableRestRouterHandler(Context & query_context_, const String & router_name) : RestRouterHandler(query_context_, router_name)
    {
    }
    ~TableRestRouterHandler() override { }

private:
    virtual String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const = 0;

protected:
    static std::map<String, std::map<String, String> >  update_schema;
    static std::map<String, String>  granularity_func_mapping;

    static String getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity);

    String buildResponse() const;
    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;

    String processQuery(const String & query) const;

    bool validateGet(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
};

}
