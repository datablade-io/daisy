#include "PlacementService.h"

#include "CatalogService.h"
#include "CommonUtils.h"

#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <common/logger_useful.h>

#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
/// globals
String SYSTEM_ROLES_KEY = "system_settings.system_roles";
String PLACEMENT_ROLE = "placement";

String PLACEMENT_KEY_PREFIX = "system_settings.system_node_metrics_dwal.";
String PLACEMENT_NAME_KEY = PLACEMENT_KEY_PREFIX + "name";
String PLACEMENT_REPLICATION_FACTOR_KEY = PLACEMENT_KEY_PREFIX + "replication_factor";
String PLACEMENT_DEFAULT_TOPIC = "__system_node_metrics";
}

PlacementService & PlacementService::instance(Context & context)
{
    static PlacementService placement{context};
    return placement;
}

PlacementService::PlacementService(Context & global_context_)
    : global_context(global_context_)
    , catalog(CatalogService::instance(global_context_))
    , dwal(DistributedWriteAheadLogPool::instance().getDefault())
    , log(&Poco::Logger::get("PlacementService"))
{
    init();
}

PlacementService::~PlacementService()
{
    shutdown();
}

void PlacementService::shutdown()
{
    if (stopped.test_and_set())
    {
        /// already shutdown
        return;
    }

    LOG_INFO(log, "PlacementService is stopping");
    if (placement)
    {
        placement->wait();
    }
    LOG_INFO(log, "PlacementService stopped");
}

void PlacementService::processMetrics(const IDistributedWriteAheadLog::RecordPtrs & records) const
{
    (void)records;
}

void PlacementService::backgroundMetrics()
{
    createDWal(dwal, placement_ctx, stopped, log);

    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(placement_ctx);

    /// FIXME, checkpoint
    while (!stopped.test())
    {
        auto result{dwal->consume(100, 500, placement_ctx)};
        if (result.err != ErrorCodes::OK)
        {
            LOG_ERROR(log, "Failed to consume data, error={}", result.err);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        if (result.records.empty())
        {
            continue;
        }

        processMetrics(result.records);
    }
}

void PlacementService::init()
{
    const auto & config = global_context.getConfigRef();

    /// if this node has `placement` role, start background thread doing placement work
    Poco::Util::AbstractConfiguration::Keys role_keys;
    config.keys(SYSTEM_ROLES_KEY, role_keys);

    for (const auto & key : role_keys)
    {
        if (config.getString(SYSTEM_ROLES_KEY + "." + key, "") == PLACEMENT_ROLE)
        {
            LOG_INFO(log, "Detects the current log has `placement` role");

            /// placement
            String topic = config.getString(PLACEMENT_NAME_KEY, PLACEMENT_DEFAULT_TOPIC);
            DistributedWriteAheadLogKafkaContext placement_kctx{topic, 1, config.getInt(PLACEMENT_REPLICATION_FACTOR_KEY, 1)};
            placement_kctx.partition = 0;

            placement.emplace(1);
            placement->scheduleOrThrowOnError([this] { backgroundMetrics(); });
            placement_ctx = placement_kctx;

            break;
        }
    }
}

}
