#pragma once

#include "MetadataService.h"


namespace DB
{
class Context;
class CatalogService;

class PlacementService final : public MetadataService
{
public:
    static PlacementService & instance(Context & global_context);

    explicit PlacementService(Context & global_context_);
    virtual ~PlacementService() = default;

    std::vector<String> place(Int32 shards, Int32 replication_factor) const;
    std::vector<String> placed(const String & table) const;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "placement"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(100, 500); }

private:
    CatalogService & catalog;
};
}
