#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>

namespace DB
{
class IngestRestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Table") { }
    ~IngestRestRouterHandler() override { }

    String execute(ReadBuffer & /* input */, HTTPServerResponse & /* response */, Int32 & /* http_status */) override
    {
        return "IngestRestRouterHandler executer not implemented";
    }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & /* payload */) const override { return false; }

    bool streaming() override { return true; }
};

}
