#pragma once

#include "RestRouterHandler.h"

namespace DB
{
struct Enrichment
{
    String time_extraction_type;
    String time_extraction_rule;
};

class IngestRawStoreHandler final : public RestRouterHandler
{
public:
    explicit IngestRawStoreHandler(Context & query_context_) : RestRouterHandler(query_context_, "IngestRawStore") { }
    ~IngestRawStoreHandler() override { }

    String execute(ReadBuffer & /* input */, HTTPServerResponse & /* response */, Int32 & /* http_status */) const override;

private:
    static std::map<String, std::map<String, String>> enrichment_schema;
    bool streaming() const override { return true; }

    static String readJSONField(ReadBuffer & buf);

    void handleEnrichment(ReadBuffer & buf) const;
};

}
