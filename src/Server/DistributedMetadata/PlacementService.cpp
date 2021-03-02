#include "PlacementService.h"
#include "CatalogService.h"

#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <random>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
/// globals
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
    : MetadataService(global_context_, "PlacementService"), catalog(CatalogService::instance(global_context_))
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

std::vector<String> PlacementService::place(Int32 shards, Int32 replication_factor, const String & /* colocated_table */) const
{
    size_t total_replicas = static_cast<size_t>(shards * replication_factor);

    std::vector<String> hosts{catalog.hosts()};
    if (hosts.size() < total_replicas)
    {
        /// Hosts are not enough
        return {};
    }

    /// FIXME, for now use randomization
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(hosts.begin(), hosts.end(), g);

    return std::vector<String>{hosts.begin(), hosts.begin() + total_replicas};
}

std::vector<String> PlacementService::placed(const String & table) const
{
    auto tables{catalog.findTableByName(table)};

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
    (void)records;

    /// checkpoint
}

}
