#include "PlacementService.h"

#include "CatalogService.h"

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
/// Globals
const String PLACEMENT_KEY_PREFIX = "system_settings.system_node_metrics_dwal.";
const String PLACEMENT_NAME_KEY = PLACEMENT_KEY_PREFIX + "name";
const String PLACEMENT_REPLICATION_FACTOR_KEY = PLACEMENT_KEY_PREFIX + "replication_factor";
const String PLACEMENT_DATA_RETENTION_KEY = PLACEMENT_KEY_PREFIX + "data_retention";
const String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";

const String THIS_HOST = getFQDNOrHostName();

template <typename Clock, typename TimeScale>
inline Int64 now()
{
    return std::chrono::duration_cast<TimeScale>(Clock::now().time_since_epoch()).count();
}

inline Int64 utcNowMilliseconds() { return now<std::chrono::system_clock, std::chrono::milliseconds>(); }
inline Int64 monotonicNowMilliseconds() { return now<std::chrono::steady_clock, std::chrono::milliseconds>(); }

}

PlacementService & PlacementService::instance(Context & context)
{
    static PlacementService placement{context};
    return placement;
}

PlacementService::PlacementService(Context & global_context_) : PlacementService(global_context_, std::make_shared<DiskStrategy>())
{
}

PlacementService::PlacementService(Context & global_context_, PlacementStrategyPtr strategy_)
    : MetadataService(global_context_, "PlacementService"), catalog(CatalogService::instance(global_context_)), strategy(strategy_)
{
}

MetadataService::ConfigSettings PlacementService::configSettings() const
{
    return {
        .name_key = PLACEMENT_NAME_KEY,
        .default_name = PLACEMENT_DEFAULT_TOPIC,
        .data_retention_key = PLACEMENT_DATA_RETENTION_KEY,
        .default_data_retention = 2,
        .replication_factor_key = PLACEMENT_REPLICATION_FACTOR_KEY,
        .request_required_acks = 1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

std::vector<NodeMetricsPtr> PlacementService::place(
    Int32 shards, Int32 replication_factor, const String & storage_policy /*= "default"*/, const String & /* colocated_table */) const
{
    size_t total_replicas = static_cast<size_t>(shards * replication_factor);
    PlacementStrategy::PlacementRequest request{total_replicas, storage_policy};

    std::shared_lock guard{rwlock};

    for (const auto & [node_identity, node_metrics] : nodes_metrics)
    {
        auto in_sync_gap = monotonicNowMilliseconds() - node_metrics->last_update_time;
        if (in_sync_gap > STALENESS_THRESHOLD_MS)
        {
            node_metrics->staled = true;
            LOG_WARNING(log, "Node {} is out of sync. The gap is {}ms", node_identity, in_sync_gap);
        }
        else
        {
            node_metrics->staled = false;
        }
        /// Update num of tables
        auto tables = catalog.findTableByNode(node_identity);
        node_metrics->num_of_tables = tables.size();
    }

    return strategy->qualifiedNodes(nodes_metrics, request);
}

std::vector<String> PlacementService::placed(const String & database, const String & table) const
{
    auto tables{catalog.findTableByName(database, table)};

    std::vector<String> hosts;
    hosts.reserve(tables.size());

    for (const auto & t : tables)
    {
        hosts.push_back(t->host);
    }
    return hosts;
}

void PlacementService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    /// Node metrics schema: node, disk_free
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);
        if (record->headers["_version"] == "1")
        {
            mergeMetrics(record->headers["_idem"], record);
        }
        else
        {
            LOG_ERROR(log, "Cannot handle version={}", record->headers["_version"]);
        }
    }
}

void PlacementService::mergeMetrics(const String & key, const IDistributedWriteAheadLog::RecordPtr & record)
{
    for (const auto & item : {"_host", "_http_port", "_tcp_port"})
    {
        if (!record->headers.contains(item))
        {
            LOG_ERROR(log, "Invalid metric record. '{}', '{}' not found", key, item);
            return;
        }
    }

    const String & host = record->headers["_host"];
    const String & http_port = record->headers["_http_port"];
    const String & tcp_port = record->headers["_tcp_port"];
    const UInt64 & broadcast_time = std::stoul(record->headers["_broadcast_time"]);

    DiskSpace disk_space;
    for (size_t row = 0; row < record->block.rows(); ++row)
    {
        const auto & policy_name = record->block.getByName("policy_name").column->getDataAt(row);
        const auto space = record->block.getByName("disk_space").column->get64(row);
        LOG_TRACE(log, "Receive disk space data from {}. Storage policy={}, Disk size={}GB", key, policy_name, space);
        disk_space.emplace(policy_name, space);
    }

    std::unique_lock guard(rwlock);

    NodeMetricsPtr node_metrics;
    auto iter = nodes_metrics.find(key);
    if (iter == nodes_metrics.end())
    {
        /// New node metrics.
        node_metrics = std::make_shared<NodeMetrics>(host);
        nodes_metrics.emplace(key, node_metrics);
    }
    else
    {
        /// Existing node metrics.
        node_metrics = iter->second;
        auto local_now = utcNowMilliseconds();
        if (static_cast<UInt64>(local_now) < broadcast_time)
        {
            LOG_WARNING(
                log,
                "The broadcast time from node identity={} host={} is ahead of local time. Clocks between the machines are out of sync.",
                key,
                host);
        }
        else if (auto latency = local_now - broadcast_time; latency > LATENCY_THRESHOLD_MS)
        {
            LOG_WARNING(
                log,
                "It took {}ms to broadcast node metrics from node identity={} host={}. Probably there is some perf issue or the clocks "
                "between the machines are out of sync too much.",
                latency,
                key,
                host);
        }
    }
    node_metrics->node_identity = key;
    node_metrics->http_port = http_port;
    node_metrics->tcp_port = tcp_port;
    node_metrics->disk_space.swap(disk_space);
    node_metrics->broadcast_time = broadcast_time;
    node_metrics->last_update_time = monotonicNowMilliseconds();
}

void PlacementService::broadcast()
{
    if (!global_context.isDistributed())
    {
        return;
    }

    auto task_holder = global_context.getSchedulePool().createTask("PlacementBroadcast", [this]() { this->broadcastTask(); });
    broadcast_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*broadcast_task)->activate();
    (*broadcast_task)->schedule();
}

void PlacementService::broadcastTask()
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get(getTypeName(TypeIndex::String));
    auto uint64_type = data_type_factory.get(getTypeName(TypeIndex::UInt64));

    auto policy_name_col = string_type->createColumn();
    auto disk_space_col = uint64_type->createColumn();
    auto * disk_space_col_inner = typeid_cast<ColumnUInt64 *>(disk_space_col.get());

    for (const auto & [policy_name, policy_ptr] : global_context.getPoliciesMap())
    {
        const auto disk_space = policy_ptr->getMaxUnreservedFreeSpace() / (1024 * 1024 * 1024);
        policy_name_col->insertData(policy_name.data(), policy_name.size());
        disk_space_col_inner->insertValue(disk_space);
        LOG_TRACE(log, "Append disk metrics {}, {}, {} GB.", global_context.getNodeIdentity(), policy_name, disk_space);
    }

    Block block;
    ColumnWithTypeAndName policy_name_col_with_type(std::move(policy_name_col), string_type, "policy_name");
    block.insert(policy_name_col_with_type);
    ColumnWithTypeAndName disk_space_col_with_type{std::move(disk_space_col), uint64_type, "disk_space"};
    block.insert(disk_space_col_with_type);

    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block)};
    record.partition_key = 0;
    record.setIdempotentKey(global_context.getNodeIdentity());
    record.headers["_host"] = THIS_HOST;
    record.headers["_http_port"] = global_context.getConfigRef().getString("http_port", "8123");
    record.headers["_tcp_port"] = global_context.getConfigRef().getString("tcp_port", "9000");
    record.headers["_broadcast_time"] = std::to_string(utcNowMilliseconds());
    record.headers["_version"] = "1";

    const auto & result = dwal->append(record, dwal_append_ctx);
    if (result.err == ErrorCodes::OK)
    {
        LOG_DEBUG(log, "Appended {} disk space records in one node metrics block", record.block.rows());
    }
    else
    {
        LOG_ERROR(log, "Failed to append node metrics block, error={}", result.err);
    }

    (*broadcast_task)->scheduleAfter(RESCHEDULE_INTERNAL_MS);
}

}
