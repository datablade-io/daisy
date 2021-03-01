#pragma once

#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

#include <any>
#include <optional>


namespace DB
{
class Context;

class MetadataService : private boost::noncopyable
{

public:
    MetadataService(Context & global_context_, const String & service_name);
    virtual ~MetadataService();
    void init();
    void shutdown();

private:
    void tailingRecords();
    virtual void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) = 0;
    virtual String role() const = 0;
    virtual String cleanupPolicy() const { return "delete"; }
    virtual std::pair<Int32, Int32> batchSizeAndTimeout() const { return std::make_pair(100, 500); }

    /// create DWal on server
    void createDWal();

protected:
    struct ConfigSettings
    {
        String name_key;
        String default_name;
        String data_retention_key;
        Int32 default_data_retention;
        String replication_factor_key;
        String auto_offset_reset = "earliest";
    };
    virtual ConfigSettings configSettings() const = 0;

protected:
    Context & global_context;

    std::any dwal_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> pool;

    Poco::Logger * log;
};
}
