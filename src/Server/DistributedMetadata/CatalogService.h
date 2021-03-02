#pragma once

#include "MetadataService.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>

#include <boost/functional/hash.hpp>


namespace DB
{
class Context;

class CatalogService final : public MetadataService
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
    virtual ~CatalogService() override = default;

    /// `broadcast` broadcasts the metadata catalog on this node
    /// to CatalogService role nodes
    void broadcast();

    std::vector<TablePtr> findTableByName(const String & table) const;
    std::vector<TablePtr> findTableByHost(const String & host) const;
    bool tableExists(const String & table) const;
    std::vector<TablePtr> tables() const;
    std::vector<String> hosts() const;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "catalog"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(1000, 200); }

private:
    void doBroadcast();
    void processQuery(BlockInputStreamPtr & in);
    void processQueryWithProcessors(QueryPipeline & pipeline);
    void append(Block && block);

private:
    using Pair = std::pair<String, Int32>; /// (table name, shard number)
    using TableInnerContainer = std::unordered_map<Pair, TablePtr, boost::hash<Pair>>;
    using TableContainer = std::unordered_map<String, TableInnerContainer>;

    TableInnerContainer buildCatalog(const String & host, const Block & bock);
    void mergeCatalog(const String & host, TableInnerContainer snapshot);

private:
    mutable std::shared_mutex rwlock;
    /// indexed by table name which is enfored to
    /// be unique across the whole system / cluster
    TableContainer indexedByName; /// (tablename, (host, shard))
    TableContainer indexedByHost; /// (host, (tablename, shard))
};
}
