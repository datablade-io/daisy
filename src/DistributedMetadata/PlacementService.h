#pragma once

#include "MetadataService.h"
#include "PlacementStrategy.h"

#include <Core/BackgroundSchedulePool.h>

namespace DB
{
class Context;
class CatalogService;

class PlacementService final : public MetadataService
{
public:
    static PlacementService & instance(Context & global_context);

    explicit PlacementService(Context & global_context_);
    explicit PlacementService(Context & global_context_, PlacementStrategyPtr strategy_);
    virtual ~PlacementService() override = default;

    std::vector<String>
    place(Int32 shards, Int32 replication_factor, const String & storage_policy = "default", const String & colocated_table = "") const;
    std::vector<String> placed(const String & database, const String & table) const;

private:
    void startupService() override { broadcast(); }
    void shutdownService() override
    {
        if (broadcast_task)
            (*broadcast_task)->deactivate();
    }
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "placement"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(100, 500); }

private:
    void mergeMetrics(const String & host, const String & http_port, const String & tcp_port, DiskSpace & disk_space);

    /// `broadcast` broadcasts the metrics of this node
    void broadcast();
    void broadcastTask();

private:
    mutable std::shared_mutex rwlock;
    CatalogService & catalog;
    NodeMetricsContainer nodes_metrics;
    PlacementStrategyPtr strategy;
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> broadcast_task;
    static constexpr size_t reschedule_internal_ms = 5000;
};

}
