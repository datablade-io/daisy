#pragma once

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <any>


namespace DB
{
class Context;

class CatalogService : private boost::noncopyable
{
public:
    struct Table
    {
        String host;
        String database;
        String name;
        String uuid;
        String engine;
        String metadata_path;
        String data_paths;
        String dependencies_database;
        String dependencies_table;
        String create_table_query;
        String engine_full;
        String partition_key;
        String sorting_key;
        String primary_key;
        String sampling_key;
        String storage_policy;
        UInt64 total_rows = 0;
        UInt64 total_bytes = 0;
        UInt8 is_temporary = 0;
        Int32 shard = 0;

        explicit Table(const String & host_) : host(host_) { }
    };

    using TablePtr = std::shared_ptr<Table>;

public:
    static CatalogService & instance(Context & context_);

    explicit CatalogService(Context & context_);
    ~CatalogService();
    void shutdown();

    /// `broadcast` broadcasts the metadata catalog on this node
    /// to CatalogService role nodes
    void broadcast();

    std::vector<TablePtr> findTableByName(const String & table) const;
    std::vector<TablePtr> findTableByHost(const String & host) const;
    std::vector<TablePtr> tables() const;

private:
    void init();
    void doBroadcast();
    void processQuery(BlockInputStreamPtr & in);
    void processQueryWithProcessors(QueryPipeline & pipeline);
    void commit(Block && block);

private:
    using Pair = std::pair<String, Int32>;
    using TableInnerContainer = std::unordered_map<Pair, TablePtr, boost::hash<Pair>>;
    using TableContainer = std::unordered_map<String, TableInnerContainer>;

    TableInnerContainer buildCatalog(const String & host, const Block & bock);
    void mergeCatalog(const String & host, TableInnerContainer snapshot);
    void buildCatalogs(const IDistributedWriteAheadLog::RecordPtrs & records);

    void backgroundCataloger();

private:
    /// global context
    Context & global_context;

    std::any catalog_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> cataloger;

    mutable std::shared_mutex rwlock;
    /// indexed by table name which is enfored to
    /// be unique across the whole system / cluster
    TableContainer indexedByName; /// (tablename, (host, shard))
    TableContainer indexedByHost; /// (host, (tablename, shard))

    Poco::Logger * log;
};
}
