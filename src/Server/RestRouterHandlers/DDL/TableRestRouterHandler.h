#pragma once

#include "Server/RestRouterHandlers/RestRouterHandler.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>

#include <boost/functional/hash.hpp>

namespace DB
{
class TableRestRouterHandler final : public RestRouterHandler
{
public:
    TableRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table") { }
    ~TableRestRouterHandler() override { }

private:
    void parseURL(const Poco::Path & path) override;

    bool validateGet(const Poco::JSON::Object::Ptr & payload) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload) const override;

    String executeGet(const Poco::JSON::Object::Ptr &  payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const override;
    String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;
    static std::map<String, std::map<String, String>> update_schema;

    String getColumnsDefination(const Poco::JSON::Array::Ptr & columns, const String & time_column) const;
    String getColumnDefination(const Poco::JSON::Object::Ptr & column) const;

    String processQuery(const String & query, Int32 & /* http_status */) const;

private:
    String database_name;
    String table_name;
};

}
