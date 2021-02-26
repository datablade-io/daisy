#include "CatalogService.h"

#include <Core/Block.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <IO/WriteBufferFromString.h>
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

namespace ErrorCodes
{
    extern int OK;
}

namespace
{
/// globals
String CATALOG_KEY_PREFIX = "system_settings.system_catalog_dwal.";
String CATALOG_NAME_KEY = CATALOG_KEY_PREFIX + "name";
String CATALOG_REPLICATION_FACTOR_KEY = CATALOG_KEY_PREFIX + "replication_factor";
String SYSTEM_ROLES_KEY = "system_settings.system_roles";

String CATALOG_ROLE = "catalog";
String CATALOG_DEFAULT_TOPIC = "__system_catalogs";

String DDL_KEY_PREFIX = "system_settings.system_ddl_dwal.";
String DDL_NAME_KEY = DDL_KEY_PREFIX + "name";
String DDL_REPLICATION_FACTOR_KEY = DDL_KEY_PREFIX + "replication_factor";
String DDL_DEFAULT_TOPIC = "__system_ddls";


inline String nodeIdentity() { return getFQDNOrHostName(); }
}

CatalogService::CatalogService(Context & global_context_)
    : global_context(global_context_)
    , dwal(DistributedWriteAheadLogPool::instance().getDefault())
    , log(&Poco::Logger::get("CatalogService"))
{
    init();
    broadcast();
}

CatalogService::~CatalogService()
{
    stopped.test_and_set();

    if (cataloger)
    {
        cataloger->wait();
    }
}

void CatalogService::broadcast()
{
    try
    {
        doBroadcast();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to execute table query, error={}", getCurrentExceptionMessage(true, true));
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
        LOG_ERROR(log, "Failed to execute table query");
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
        auto result = dwal->append(record, catalog_ctx);
        if (result.err == 0)
        {
            return;
        }

        LOG_ERROR(log, "Failed to commit, error={}", result.err);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
}

void CatalogService::buildCatalog(const String & host, const Block & block)
{
    for (size_t row = 0; row < block.rows(); ++row)
    {
        Table table{host};
        std::unordered_map<String, void *> kvp = {
            {"database", &table.database},
            {"name", &table.name},
            {"engine", &table.engine},
            {"metadata_path", &table.metadata_path},
            {"data_paths", &table.data_paths},
            {"dependencies_database", &table.dependencies_database},
            {"dependencies_table", &table.dependencies_table},
            {"create_table_query", &table.create_table_query},
            {"engine_full", &table.engine_full},
            {"partition_key", &table.partition_key},
            {"sorting_key", &table.sorting_key},
            {"primary_key", &table.primary_key},
            {"sampling_key", &table.sampling_key},
            {"storage_policy", &table.storage_policy},
            {"total_rows", &table.total_rows},
            {"total_bytes", &table.total_bytes},
        };

        for (const auto & col : block)
        {
            auto it = kvp.find(col.name);
            if (it != kvp.end())
            {
                if (col.name == "total_rows" || col.name == "total_bytes")
                {
                    *static_cast<UInt64 *>(it->second) = col.column->get64(row);
                }
                else if (col.name == "data_paths" || col.name == "dependencies_database" || col.name == "dependencies_table")
                {
                    WriteBufferFromOwnString buffer;
                    col.type->serializeAsText(*col.column->assumeMutable().get(), row, buffer, FormatSettings{});
                    *static_cast<String *>(it->second) = buffer.str();
                }
                else
                {
                    *static_cast<String *>(it->second) = col.column->getDataAt(row).toString();
                }
            }
        }

        /// FIXME; shard parsing

        {
            bool replaced = false;
            std::unique_lock guard(rwlock);
            for (auto & tbl: tables[table.name])
            {
                if ((tbl.shard == table.shard) && (tbl.host == table.host))
                {
                    tbl = table;
                    replaced = true;
                    break;
                }
            }

            if (!replaced)
            {
                tables[table.name].push_back(std::move(table));
            }
        }
    }

    /// std::shared_lock guard(rwlock);
}

void CatalogService::buildCatalogs(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);

        buildCatalog(record->idempotent_key, record->block);
    }
}

void CatalogService::backgroundCataloger()
{
    createDWal(catalog_ctx);

    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(catalog_ctx);

    /// always consuming from beginning
    while (!stopped.test())
    {
        auto result{dwal->consume(1000, 200, catalog_ctx)};
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

        buildCatalogs(result.records);
    }
}

bool CatalogService::validateSchema(const Block & block, const std::vector<String> & col_names)
{
    for (const auto & col_name : col_names)
    {
        if (!block.has(col_name))
        {
            LOG_ERROR(log, "`{}` column is missing", col_name);
            return false;
        }
    }
    return true;
}

void CatalogService::createTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }

    String query = block.getByName("ddl").column->getDataAt(0).toString();
    (void)query;
}

void CatalogService::deleteTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }
}

void CatalogService::alterTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }
}

void CatalogService::processDDL(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        if (record->op_code == IDistributedWriteAheadLog::OpCode::CREATE_TABLE)
        {
            createTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::DELETE_TABLE)
        {
            deleteTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::ALTER_TABLE)
        {
            alterTable(record->block);
        }
        else
        {
            assert(0);
            LOG_ERROR(log, "Unknown operation={}", static_cast<Int32>(record->op_code));
        }
    }
}

void CatalogService::backgroundDDL()
{
    createDWal(ddl_ctx);

    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ddl_ctx);

    /// FIXME, checkpoint
    while (!stopped.test())
    {
        auto result{dwal->consume(10, 200, catalog_ctx)};
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

        processDDL(result.records);
    }
}

/// try indefinitely to create dwal for catalog
void CatalogService::createDWal(std::any & ctx)
{
    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (dwal->describe(kctx.topic, ctx) == ErrorCodes::OK)
    {
        return;
    }

    LOG_INFO(log, "Catalog topic={} is not detected, create one", kctx.topic);
    while (!stopped.test())
    {
        if (dwal->create(kctx.topic, ctx) == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully created catalog topic={}", kctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to create catalog topic={}, will retry indefinitely ...", kctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void CatalogService::init()
{
    const auto & config = global_context.getConfigRef();

    /// if this node has `catalog` role, start background thread doing catalog work
    Poco::Util::AbstractConfiguration::Keys role_keys;
    config.keys(SYSTEM_ROLES_KEY, role_keys);

    for (const auto & key : role_keys)
    {
        if (config.getString(SYSTEM_ROLES_KEY + "." + key, "") == CATALOG_ROLE)
        {
            LOG_INFO(log, "Detects the current log has `catalog` role");

            /// catalog
            String catalog_topic = config.getString(CATALOG_NAME_KEY, CATALOG_DEFAULT_TOPIC);
            DistributedWriteAheadLogKafkaContext catalog_kctx{catalog_topic, 1, config.getInt(CATALOG_REPLICATION_FACTOR_KEY, 1)};
            catalog_kctx.partition = 0;

            cataloger.emplace(1);
            cataloger->scheduleOrThrowOnError([this] { backgroundCataloger(); });
            catalog_ctx = catalog_kctx;

            /// ddl
            String ddl_topic = config.getString(DDL_NAME_KEY, DDL_DEFAULT_TOPIC);
            DistributedWriteAheadLogKafkaContext ddl_kctx{ddl_topic, 1, config.getInt(DDL_REPLICATION_FACTOR_KEY, 1)};
            ddl_kctx.partition = 0;

            ddl.emplace(1);
            ddl->scheduleOrThrowOnError([this] { backgroundDDL(); });
            ddl_ctx = ddl_kctx;

            break;
        }
    }
}
}
