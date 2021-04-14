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
    explicit TableRestRouterHandler(Context & query_context_, const String & router_name = "Table");
    ~TableRestRouterHandler() override { }

private:
    virtual bool validateGet(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    virtual bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    virtual bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const;
    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;

    String getTimeColumn(const Poco::JSON::Object::Ptr & payload) const;

    virtual String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & time_column) const;

protected:
    std::map<String, std::map<String, String> > & create_schema;
    std::map<String, std::map<String, String> > & column_schema;
    std::map<String, std::map<String, String> > & update_schema;
    std::map<String, String> & granularity_func_mapping;

    String buildResponse() const;
    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;
    String getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity) const;
    String processQuery(const String & query) const;

    virtual String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
};

}
