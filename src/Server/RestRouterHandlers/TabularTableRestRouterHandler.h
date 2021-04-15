#pragma once

#include "TableRestRouterHandler.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Processors/QueryPipeline.h>

#include <boost/functional/hash.hpp>
#include <Poco/Util/AbstractConfiguration.h>

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

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const;
    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;
    String getTimeColumn(const Poco::JSON::Object::Ptr & payload) const;
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const override;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & time_column) const;
};

}
