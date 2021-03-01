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

    DistributedWriteAheadLogPtr get(const String & id) const;

    DistributedWriteAheadLogPtr getDefault() const;

private:
    void doInit(const Context & global_context, const String & key);
    void init(Context & global_context);

private:
    DistributedWriteAheadLogPtr default_wal;
    std::unordered_map<String, std::vector<DistributedWriteAheadLogPtr>> wals;
    mutable std::unordered_map<String, std::atomic_uint64_t> indexes;

    Poco::Logger * log;
};
}
