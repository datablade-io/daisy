#pragma once

#include "RestRouterHandler.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Processors/QueryPipeline.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <boost/functional/hash.hpp>

namespace DB
{
class TableRestRouterHandler final : public RestRouterHandler
{
public:
    explicit TableRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table"){}
    ~TableRestRouterHandler() override { }

private:
    bool validateGet(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const;
    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;

    String buildResponse() const;
    String processQuery(const String & query) const;


    String getTableCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const;
    String getTimeColumn(const Poco::JSON::Object::Ptr & payload) const;
    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;
    String getPartitionExpr(const Poco::JSON::Object::Ptr & payload) const;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & time_column) const;
};

}
