#include "CatalogService.h"

#include <Core/Block.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Common/Exception.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace
{
/// globals
String CATALOG_KEY_PREFIX = "system_settings.system_catalog_dwal.";
String CATALOG_NAME_KEY = CATALOG_KEY_PREFIX + "name";
String CATALOG_REPLICATION_FACTOR_KEY = CATALOG_KEY_PREFIX + "replication_factor";
String SYSTEM_ROLES_KEY = "system_settings.system_roles";
String CATALOG_ROLE = "catalog";
String CATALOG_DEFAULT_TOPIC = "__system_catalog";

inline String nodeIdentity() { return getFQDNOrHostName(); }
}

CatalogService::CatalogService(Context & global_context_)
    : global_context(global_context_)
    , dwal(DistributedWriteAheadLogPool::instance().getDefault())
    , cataloger(1)
    , log(&Poco::Logger::get("CatalogService"))
{
    init();
    broadcast();
}

void CatalogService::broadcast()
{
    try
    {
        doBroadcast();
    }
    catch (...)
    {
        LOG_ERROR(log, "CatalogService failed to execute table query, error={}", getCurrentExceptionMessage(true, true));
    }
}

void CatalogService::doBroadcast()
{
    assert(dwal);

    String query = "SELECT * FROM system.tables WHERE database != 'system'";

    /// CurrentThread::attachQueryContext(context);
    Context context = global_context;
    context.makeQueryContext();
    BlockIO io{executeQuery(query, context)};

    if (io.pipeline.initialized())
    {
        processQueryWithProcessors(io.pipeline);
    }
    else if (io.in)
    {
        processQuery(io.in);
    }
    else
    {
        assert(false);
        LOG_ERROR(log, "CatalogService failed to execute table query");
    }
}

void CatalogService::processQueryWithProcessors(QueryPipeline & pipeline)
{
    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;

    while (executor.pull(block, 100))
    {
        if (block)
        {
            commit(std::move(block));
        }
    }
}

void CatalogService::processQuery(BlockInputStreamPtr & in)
{
    AsynchronousBlockInputStream async_in(in);
    async_in.readPrefix();

    while (true)
    {
        if (async_in.poll(100))
        {
            Block block{async_in.read()};
            if (!block)
            {
                break;
            }

            commit(std::move(block));
        }
    }

    async_in.readSuffix();
}

void CatalogService::commit(Block && block)
{
    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block)};
    record.idempotent_key = nodeIdentity();

    /// FIXME : reschedule
    int retries = 3;
    while (retries--)
    {
        auto result = dwal->append(record, ctx);
        if (result.err == 0)
        {
            return;
        }

        LOG_ERROR(log, "CatalogService failed to commit, error={}", result.err);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
}

void CatalogService::backgroundCataloger(const String & topic, Int32 replication_factor)
{
    (void)topic;
    (void)replication_factor;
}

void CatalogService::init()
{
    const auto & config = global_context.getConfigRef();
    String topic = config.getString(CATALOG_NAME_KEY, CATALOG_DEFAULT_TOPIC);
    ctx = DistributedWriteAheadLogKafkaContext{topic};

    /// if this node has `catalog` role, start background thread doing catalog work
    Poco::Util::AbstractConfiguration::Keys role_keys;
    config.keys(SYSTEM_ROLES_KEY, role_keys);

    for (const auto & key : role_keys)
    {
        if (config.getString(SYSTEM_ROLES_KEY + "." + key, "") == CATALOG_ROLE)
        {
            LOG_INFO(log, "CatalogService detects the current log has `catalog` role");
            auto replication_factor = config.getInt(CATALOG_REPLICATION_FACTOR_KEY, 1);
            cataloger.scheduleOrThrowOnError([this, topic, replication_factor] { backgroundCataloger(topic, replication_factor); });
        }
    }
}
}
