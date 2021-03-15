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
        /// `host` is network reachable like hostname, FQDN or IP
        String host;
        /// host_identity can be unique uuid
        String host_identity;

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
    using TablePtrs = std::vector<std::shared_ptr<Table>>;

public:
    static CatalogService & instance(Context & context_);

    explicit CatalogService(Context & context_);
    virtual ~CatalogService() override = default;

    /// `broadcast` broadcasts the metadata catalog on this node
    /// to CatalogService role nodes
    void broadcast();

    std::vector<TablePtr> findTableByName(const String & database, const String & table) const;
    std::vector<TablePtr> findTableByHost(const String & host) const;
    std::vector<TablePtr> findTableByDB(const String & database) const;

    bool tableExists(const String & database, const String & table) const;

    std::vector<TablePtr> tables() const;
    std::vector<String> databases() const;

    /// FIXME : remove `hosts` when Placement service is ready
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
    using HostShard = std::pair<String, Int32>;
    using TableShard = std::pair<String, Int32>;
    using DatabaseTable = std::pair<String, String>;
    using DatabaseTableShard = std::pair<String, TableShard>;

    /// In the cluster, (database, table, shard) is unique
    using TableContainerPerHost = std::unordered_map<DatabaseTableShard, TablePtr, boost::hash<DatabaseTableShard>>;

    TableContainerPerHost buildCatalog(const String & host, const Block & bock);
    void mergeCatalog(const String & host, TableContainerPerHost snapshot);

private:
    using TableContainerByHostShard = std::unordered_map<HostShard, TablePtr, boost::hash<HostShard>>;

    mutable std::shared_mutex rwlock;

    /// (database, table) -> ((host, shard) -> TablePtr))
    std::unordered_map<DatabaseTable, TableContainerByHostShard, boost::hash<DatabaseTable>> indexedByName;

    /// host -> ((database, table, shard) -> TablePtr))
    std::unordered_map<String, TableContainerPerHost> indexedByHost;
};
}
