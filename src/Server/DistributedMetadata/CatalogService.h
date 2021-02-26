#pragma once

#include <boost/noncopyable.hpp>

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <any>


namespace DB
{
class Context;

class CatalogService : private boost::noncopyable
{
public:
    explicit CatalogService(Context & context_);
    ~CatalogService();

    /// `broadcast` broadcasts the metadata catalog on this node
    /// to CatalogService role nodes
    void broadcast();

private:
    void init();
    void doBroadcast();
    void processQuery(BlockInputStreamPtr & in);
    void processQueryWithProcessors(QueryPipeline & pipeline);
    void commit(Block && block);

    void buildCatalog(const String & host, const Block & bock);
    void createTable(const Block & bock);
    void deleteTable(const Block & bock);
    void alterTable(const Block & bock);
    void process(const IDistributedWriteAheadLog::RecordPtrs & records);

    void backgroundCataloger();
    void createDWal();

private:
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

private:
    /// global context
    Context & global_context;

    std::any ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> cataloger;

    mutable std::shared_mutex rwlock;
    /// indexed by table name which is enfored to
    /// be unique across the whole system / cluster
    std::unordered_map<String, std::vector<Table>> tables;

    Poco::Logger * log;
};
}
