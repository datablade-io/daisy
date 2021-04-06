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

    std::vector<NodeMetricsPtr>
    place(Int32 shards, Int32 replication_factor, const String & storage_policy = "default", const String & colocated_table = "") const;
    std::vector<String> placed(const String & database, const String & table) const;

private:
    void postStartup() override { broadcast(); }
    void preShutdown() override
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
    void mergeMetrics(const String & key, const IDistributedWriteAheadLog::RecordPtr & record);

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
    static constexpr Int64 latency_threshold_ms = 5000;
    static constexpr Int64 in_sync_threshold_ms = 10000;
};

}
