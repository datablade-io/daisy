#pragma once

#include "IDistributedWriteAheadLog.h"


namespace DB
{

/// Pooling DistributedWriteAheadLog. Singleton
class DistributedWriteAheadLogPool : private boost::noncopyable
{
public:
    static DistributedWriteAheadLogPool & instance()
    {
        static DistributedWriteAheadLogPool pool;
        return pool;
    }

    DistributedWriteAheadLogPtr getWriteAheadLog(const String & id, std::function<DistributedWriteAheadLogPtr()> create_func, Int32 size)
    {
        std::lock_guard<std::mutex> guard(lock);
        auto iter = wals.find(id);
        if (iter == wals.end())
        {
            for (Int32 i = 0; i < size; ++i)
            {
                wals[id].push_back(create_func());
            }

            indexes[id] = 0;
            iter = wals.find(id);
        }

        return iter->second[indexes[id]++ % iter->second.size()];
    }

    void setDefault(DistributedWriteAheadLogPtr default_wal_)
    {
        std::lock_guard<std::mutex> guard(lock);
        default_wal = default_wal_;
    }

    DistributedWriteAheadLogPtr getDefault()
    {
        std::lock_guard<std::mutex> guard(lock);
        return default_wal;
    }

private:
    std::mutex lock;
    DistributedWriteAheadLogPtr default_wal;
    std::unordered_map<String, std::vector<DistributedWriteAheadLogPtr>> wals;
    std::unordered_map<String, size_t> indexes;
};
}
