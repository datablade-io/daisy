#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class ColumnRestRouterHandler final : public RestRouterHandler
{
public:
    explicit ColumnRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Column") { }
    ~ColumnRestRouterHandler() override { }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

private:
    std::pair<bool, String> columnExist(const String & database_name, const String & table_name, const String & column_name) const;
};

}
