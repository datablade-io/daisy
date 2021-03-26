#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class IngestRestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestRestRouterHandler(Context & query_context_) : RestRouterHandler(query_context_, "Ingest") { }
    ~IngestRestRouterHandler() override { }

    String execute(ReadBuffer & /* input */, HTTPServerResponse & /* response */, Int32 & http_status) override;

private:
    bool streaming() const override { return true; }
};

}
