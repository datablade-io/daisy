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
    explicit TableRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table")
    {
        topic = query_context_.getGlobalContext().getConfigRef().getString("system_settings.system_ddl_dwal.name");
    }
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
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;
    static std::map<String, std::map<String, String>> update_schema;
    String topic;

    String getColumnsDefination(const Poco::JSON::Array::Ptr & columns, const String & time_column) const;
    String getColumnDefination(const Poco::JSON::Object::Ptr & column) const;

    Block buildBlock(const std::vector<std::pair<String, String>> & string_cols) const;
    void appendRecord(IDistributedWriteAheadLog::Record & record) const;
    String buildResponse() const;
    String processQuery(const String & query, Int32 & http_status) const;
};

}
