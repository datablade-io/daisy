#include "CatalogService.h"

#include <Core/Block.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int OK;
}

namespace
{
    /// Globals
    const String CATALOG_KEY_PREFIX = "system_settings.system_catalog_dwal.";
    const String CATALOG_NAME_KEY = CATALOG_KEY_PREFIX + "name";
    const String CATALOG_REPLICATION_FACTOR_KEY = CATALOG_KEY_PREFIX + "replication_factor";
    const String CATALOG_DATA_RETENTION_KEY = CATALOG_KEY_PREFIX + "data_retention";
    const String CATALOG_DEFAULT_TOPIC = "__system_catalogs";
}

CatalogService & CatalogService::instance(Context & context)
{
    static CatalogService catalog{context};
    return catalog;
}


CatalogService::CatalogService(Context & global_context_) : MetadataService(global_context_, "CatalogService")
{
}

MetadataService::ConfigSettings CatalogService::configSettings() const
{
    return {
        .name_key = CATALOG_NAME_KEY,
        .default_name = CATALOG_DEFAULT_TOPIC,
        .data_retention_key = CATALOG_DATA_RETENTION_KEY,
        .default_data_retention = -1,
        .replication_factor_key = CATALOG_REPLICATION_FACTOR_KEY,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

void CatalogService::broadcast()
{
    if (!global_context.isDistributed())
    {
        return;
    }

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

    String query = "SELECT * FROM system.tables WHERE (database != 'system') OR (database = 'system' AND name='tables')";

    /// CurrentThread::attachQueryContext(context);
    Context context = global_context;
    context.makeQueryContext();
    BlockIO io{executeQuery(query, context, true /* internal */)};

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
            append(std::move(block));
            assert(!block);
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

            append(std::move(block));
            assert(!block);
        }
    }

    async_in.readSuffix();
}

void CatalogService::append(Block && block)
{
    IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block)};
    record.partition_key = 0;
    record.idempotent_key = global_context.getNodeIdentity();

    /// FIXME : reschedule
    int retries = 3;
    while (retries--)
    {
        const auto & result = dwal->append(record, dwal_append_ctx);
        if (result.err == ErrorCodes::OK)
        {
            LOG_INFO(log, "Appended {} table definitions in one block", record.block.rows());
            return;
        }

        LOG_ERROR(log, "Failed to append table definition block, error={}", result.err);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
}

std::vector<String> CatalogService::hosts() const
{
    std::vector<String> results;

    std::shared_lock guard(rwlock);
    results.reserve(indexedByHost.size());

    for (const auto & p : indexedByHost)
    {
        results.push_back(p.first);
    }
    return results;
}

std::vector<String> CatalogService::databases() const
{
    std::unordered_set<String> dbs;

    {
        std::shared_lock guard(rwlock);
        for (const auto & p : indexedByName)
        {
            dbs.insert(p.first.first);
        }
    }

    return std::vector<String>{dbs.begin(), dbs.end()};
}

std::vector<CatalogService::TablePtr> CatalogService::tables() const
{
    std::vector<TablePtr> results;

    std::shared_lock guard(rwlock);

    for (const auto & p : indexedByName)
    {
        for (const auto & pp : p.second)
        {
            results.push_back(pp.second);
        }
    }
    return results;
}

std::vector<CatalogService::TablePtr> CatalogService::findTableByHost(const String & host) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard(rwlock);

    auto iter = indexedByHost.find(host);
    if (iter != indexedByHost.end())
    {
        for (const auto & p : iter->second)
        {
            results.push_back(p.second);
        }
    }
    return results;
}

std::vector<CatalogService::TablePtr> CatalogService::findTableByName(const String & database, const String & table) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard(rwlock);

    auto iter = indexedByName.find(std::make_pair(database, table));
    if (iter != indexedByName.end())
    {
        for (const auto & p : iter->second)
        {
            results.push_back(p.second);
        }
    }
    return results;
}

std::vector<CatalogService::TablePtr> CatalogService::findTableByDB(const String & database) const
{
    std::vector<TablePtr> results;

    std::shared_lock guard(rwlock);

    for (const auto & p : indexedByName)
    {
        if (p.first.first == database)
        {
            for (const auto & pp : p.second)
            {
                results.push_back(pp.second);
            }
        }
    }

    return results;
}

bool CatalogService::tableExists(const String & database, const String & table) const
{
    std::shared_lock guard(rwlock);

    return indexedByName.find(std::make_pair(database, table)) != indexedByName.end();
}

CatalogService::TableContainerPerHost CatalogService::buildCatalog(const String & host, const Block & block)
{
    TableContainerPerHost snapshot;

    for (size_t row = 0; row < block.rows(); ++row)
    {
        TablePtr table = std::make_shared<Table>(host);
        std::unordered_map<String, void *> kvp = {
            {"database", &table->database},
            {"name", &table->name},
            {"engine", &table->engine},
            {"metadata_path", &table->metadata_path},
            {"data_paths", &table->data_paths},
            {"dependencies_database", &table->dependencies_database},
            {"dependencies_table", &table->dependencies_table},
            {"create_table_query", &table->create_table_query},
            {"engine_full", &table->engine_full},
            {"partition_key", &table->partition_key},
            {"sorting_key", &table->sorting_key},
            {"primary_key", &table->primary_key},
            {"sampling_key", &table->sampling_key},
            {"storage_policy", &table->storage_policy},
            {"total_rows", &table->total_rows},
            {"total_bytes", &table->total_bytes},
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

        /// FIXME; parsing `shard` in `create_table_query`
        DatabaseTableShard key = std::make_pair(table->database, std::make_pair(table->name, table->shard));
        snapshot.emplace(std::move(key), std::move(table));

        assert(!table);
    }

    return snapshot;
}

/// Merge a snapshot of tables from one host
void CatalogService::mergeCatalog(const String & host, TableContainerPerHost snapshot)
{
    std::unique_lock guard(rwlock);

    auto iter = indexedByHost.find(host);
    if (iter == indexedByHost.end())
    {
        /// New host. Add all tables from this host to `indexedByName`
        for (const auto & p : snapshot)
        {
            indexedByName[std::make_pair(p.second->database, p.second->name)].emplace(
                std::make_pair(p.second->host, p.second->shard), p.second);
        }
        indexedByHost.emplace(host, snapshot);
        return;
    }

    /// Found host. Merge existing tables from this host to `indexedByName`
    /// and delete `deleted` table entries from `indexedByName`
    for (const auto & p : iter->second)
    {
        /// ((database, tablename, shard), table) pair
        if (!snapshot.contains(p.first))
        {
            auto iter_by_name = indexedByName.find(std::make_pair(p.second->database, p.second->name));
            assert(iter_by_name != indexedByName.end());

            /// Deleted table, remove from `indexByName`
            auto removed = iter_by_name->second.erase(std::make_pair(p.second->host, p.second->shard));
            assert(removed == 1);
            (void)removed;
        }
    }

    /// Add new tables or override existing tables in `indexedByName`
    for (const auto & p : snapshot)
    {
        DatabaseTable key = std::make_pair(p.second->database, p.second->name);
        auto iter_by_name = indexedByName.find(key);
        if (iter_by_name == indexedByName.end())
        {
            /// New table
            indexedByName[key].emplace(std::make_pair(p.second->host, p.second->shard), p.second);
        }
        else
        {
            /// An existing table
            indexedByName[key].insert_or_assign(std::make_pair(p.second->host, p.second->shard), p.second);
        }
    }

    /// Replace all tables for this host in `indexedByHost`
    indexedByHost[host].swap(snapshot);
}

void CatalogService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);

        mergeCatalog(record->idempotent_key, buildCatalog(record->idempotent_key, record->block));
    }
}
}
