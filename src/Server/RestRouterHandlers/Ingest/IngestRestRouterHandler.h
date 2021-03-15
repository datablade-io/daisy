#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>

namespace DB
{

class IngestRestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table") {}
    ~IngestRestRouterHandler() override {}

    String execute(ReadBuffer & input, const HTMLForm & params, Int32 & http_status) override
    {
        input.eof();
        params.empty();
        http_status = 100;
        return "IngestRestRouterHandler not implemented";
    }

private:

    void parseURL(const Poco::Path & path) override;

    bool validatePost(const Poco::JSON::Object::Ptr & payload) const override {
        payload.isNull();
        return false;
    }

    bool streaming() override { return true; }
    
private:
    String database_name;
    String table_name;
};

}
