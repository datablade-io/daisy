#pragma once

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <Poco/URI.h>

#include <boost/noncopyable.hpp>

#include <any>
#include <optional>


namespace DB
{
class Context;
class CatalogService;
class PlacementService;

class DDLService : private boost::noncopyable
{
public:
    explicit DDLService(Context & context_);
    ~DDLService();
    void shutdown();

private:
    void init();

    /// Talk to Placement service to place shard replicas
    std::vector<Poco::URI> placeReplicas(Int32 shards, Int32 replication_factor) const;
    std::vector<Poco::URI> placedReplicas(const String & table) const;

    Int32 postRequest(const String & query, const Poco::URI & uri) const;
    Int32 doTable(const String & query, const Poco::URI & uri) const;
    void createTable(const Block & bock) const;
    void mutateTable(const Block & bock) const;
    void processDDL(const IDistributedWriteAheadLog::RecordPtrs & records) const;

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;

    void backgroundDDL();

private:
    /// global context
    Context & global_context;

    CatalogService & catalog;
    PlacementService & placement;

    std::any ddl_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> ddl;

    Poco::Logger * log;
};
}
