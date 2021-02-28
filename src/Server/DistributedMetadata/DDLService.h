#pragma once

#include "MetadataService.h"

#include <Poco/URI.h>


namespace DB
{
class Context;
class CatalogService;
class PlacementService;

class DDLService : public MetadataService
{
public:
    static DDLService & instance(Context & global_context_);

    explicit DDLService(Context & context_);
    virtual ~DDLService() = default;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "ddl"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(10, 200); }

private:
    std::vector<Poco::URI> place(const std::function<std::vector<String>()> & func) const;

    /// Talk to Placement service to place shard replicas
    std::vector<Poco::URI> placeReplicas(Int32 shards, Int32 replication_factor) const;
    std::vector<Poco::URI> placedReplicas(const String & table) const;

    Int32 postRequest(const String & query, const Poco::URI & uri) const;
    Int32 doTable(const String & query, const Poco::URI & uri) const;
    void createTable(const Block & bock) const;
    void mutateTable(const Block & bock) const;
    void commit(Int64 last_sn);

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;

private:
    String http_port;

    CatalogService & catalog;
    PlacementService & placement;
};
}
