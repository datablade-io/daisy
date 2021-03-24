#include "PlacementService.h"
#include <algorithm>
#include <random>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <common/logger_useful.h>
#include <common/getFQDNOrHostName.h>
#include "CatalogService.h"


namespace DB
{
namespace
{
/// Globals
String PLACEMENT_KEY_PREFIX = "system_settings.system_node_metrics_dwal.";
String PLACEMENT_NAME_KEY = PLACEMENT_KEY_PREFIX + "name";
String PLACEMENT_REPLICATION_FACTOR_KEY = PLACEMENT_KEY_PREFIX + "replication_factor";
String PLACEMENT_DATA_RETENTION_KEY = PLACEMENT_KEY_PREFIX + "data_retention";
String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";
}

PlacementService & PlacementService::instance(Context & context)
{
    static PlacementService placement{context};
    return placement;
}

PlacementService::PlacementService(Context & global_context_)
    : PlacementService(global_context_, std::dynamic_pointer_cast<PlacementStrategy>(std::make_shared<DiskStrategy>()))
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
        .auto_offset_reset = "latest",
    };
}

std::vector<String> PlacementService::place(
    Int32 shards, Int32 replication_factor, const String & storage_policy /*= "default"*/, const String & /* colocated_table */) const
{
    std::unique_lock guard(rwlock);

    size_t total_replicas = static_cast<size_t>(shards * replication_factor);
    PlacementStrategy::PlacementQuery query{total_replicas, storage_policy};
    return strategy->qualifiedHosts(hostStates, query);
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
    /// node metrics schema: host, disk_free, tables, location, timestamp
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);
        String & idempotent_key = record->headers["_idem"];

        DiskSpace disk_space;
        for (size_t row = 0; row < record->block.rows(); ++row)
        {
            const auto & policy_name = record->block.getByName("policy_name").column->getDataAt(row);
            const auto & space = record->block.getByName("disk_space").column->get64(row);
            LOG_INFO(log, "Get disk space data. Host:{}, Policy:{}, Size:{}", idempotent_key, policy_name, space);
            disk_space.emplace(policy_name, space);
        }
        mergeStates(idempotent_key, disk_space);
    }
    /// checkpoint.
}

void PlacementService::mergeStates(const String & host, DiskSpace & disk)
{
    std::unique_lock guard(rwlock);

    auto iter = hostStates.find(host);
    if (iter == hostStates.end())
    {
        /// New host metrics.
        HostStatePtr state = std::make_shared<HostState>(host);
        state->disk_space = disk;
        hostStates.emplace(host, state);
        return;
    }
    /// Update host metrics.
    iter->second->disk_space = disk;
}

void PlacementService::broadcast()
{
    auto task_holder = global_context.getSchedulePool().createTask("PlacementBroadcast", [this]() { this->broadcastTask(); });
    broadcast_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*broadcast_task)->activate();
    (*broadcast_task)->schedule();
}

void PlacementService::broadcastTask()
{
    auto string_type = std::make_shared<DataTypeString>();
    auto uint64_type = std::make_shared<DataTypeUInt64>();
    auto policy_name_col = string_type->createColumn();
    auto disk_space_col = uint64_type->createColumn();
    auto * disk_space_col_inner = typeid_cast<ColumnUInt64 *>(disk_space_col.get());

    for (const auto & [policy_name, policy_ptr] : global_context.getPoliciesMap())
    {
        const auto & disk_space = policy_ptr->getMaxUnreservedFreeSpace();
        policy_name_col->insertData(policy_name.data(), policy_name.size());
        disk_space_col_inner->insertValue(disk_space);
        LOG_INFO(log, "Append disk metrics {} {} {}", global_context.getNodeIdentity(), policy_name, disk_space);
    }

    Block block;
    ColumnWithTypeAndName policy_name_col_with_type(std::move(policy_name_col), string_type, "policy_name");
    block.insert(policy_name_col_with_type);
    ColumnWithTypeAndName disk_space_col_with_type{std::move(disk_space_col), uint64_type, "disk_space"};
    block.insert(disk_space_col_with_type);

    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block)};
    record.partition_key = 0;
    record.headers["_idem"] = global_context.getNodeIdentity();
    record.headers["_host"] = getFQDNOrHostName();
    record.headers["_http_port"] = global_context.getConfigRef().getString("http_port", "8123");
    record.headers["_tcp_port"] = global_context.getConfigRef().getString("tcp_port", "9000");

    const auto & result = dwal->append(record, dwal_append_ctx);
    if (result.err == ErrorCodes::OK)
    {
        LOG_INFO(log, "Appended {} disk space records in one block", record.block.rows());
    }
    else
    {
        LOG_ERROR(log, "Failed to append Host State block, error={}", result.err);
    }

    (*broadcast_task)->scheduleAfter(reschedule_time_ms);
}

}
