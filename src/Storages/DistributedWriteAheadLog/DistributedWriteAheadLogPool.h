#pragma once

#include "IDistributedWriteAheadLog.h"


namespace DB
{
/// Pooling DistributedWriteAheadLog. Singleton
class DistributedWriteAheadLogPool : private boost::noncopyable
{
public:
    static DistributedWriteAheadLogPool & instance(Context & global_context);

    explicit DistributedWriteAheadLogPool(Context & global_context);
    ~DistributedWriteAheadLogPool();

    DistributedWriteAheadLogPtr get(const String & id) const;

    DistributedWriteAheadLogPtr getDefault() const;

    void startup();
    void shutdown();

private:
    void init(const String & key);

private:
    Context & global_context;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    DistributedWriteAheadLogPtr default_wal;
    std::unordered_map<String, std::vector<DistributedWriteAheadLogPtr>> wals;
    mutable std::unordered_map<String, std::atomic_uint64_t> indexes;

    Poco::Logger * log;
};
}
