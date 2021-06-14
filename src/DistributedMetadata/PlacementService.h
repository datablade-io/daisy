#pragma once

#include "MetadataService.h"
#include "PlacementStrategy.h"

#include <Core/BackgroundSchedulePool.h>

namespace DB
{
class CatalogService;

class PlacementService final : public MetadataService
{
public:
    static PlacementService & instance(const ContextPtr & global_context);

    explicit PlacementService(const ContextPtr & global_context_);
    PlacementService(const ContextPtr & global_context_, PlacementStrategyPtr strategy_);
    virtual ~PlacementService() override = default;

    void scheduleBroadcast();
    std::vector<NodeMetricsPtr>
    place(Int32 shards, Int32 replication_factor, const String & storage_policy = "default", const String & colocated_table = "") const;
    std::vector<String> placed(const String & database, const String & table) const;
    String getNodeIdentityByChannel(const String & channel) const;
    std::vector<NodeMetricsPtr> nodes() const;

private:
    void preShutdown() override;
    void processRecords(const DWAL::RecordPtrs & records) override;
    String role() const override { return "placement"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(100, 500); }

private:
    void mergeMetrics(const String & key, const DWAL::RecordPtr & record);

    /// `broadcast` broadcasts the metrics of this node
    void broadcast();
    void doBroadcast();

private:
    CatalogService & catalog;

    mutable std::shared_mutex rwlock;
    NodeMetricsContainer nodes_metrics;

    PlacementStrategyPtr strategy;
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> broadcast_task;

    size_t reschedule_interval;
};

}
