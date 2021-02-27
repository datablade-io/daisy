#pragma once

#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

#include <any>
#include <optional>


namespace DB
{
class Context;
class CatalogService;


class PlacementService : private boost::noncopyable
{
public:
    static PlacementService & instance(Context & context);
    explicit PlacementService(Context & global_context_);
    ~PlacementService();

    void shutdown();

private:
    void init();
    void processMetrics(const IDistributedWriteAheadLog::RecordPtrs & records) const;
    void backgroundMetrics();

private:
    Context & global_context;

    CatalogService & catalog;

    std::any placement_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> placement;

    Poco::Logger * log;
};
}
