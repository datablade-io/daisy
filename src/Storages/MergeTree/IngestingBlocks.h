#pragma once

#include <Core/Types.h>
#include <Poco/Logger.h>

#include <boost/core/noncopyable.hpp>

#include <chrono>
#include <deque>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>


namespace DB
{
class IngestingBlocks : public boost::noncopyable
{
public:
    static IngestingBlocks & instance();

public:
    IngestingBlocks();
    ~IngestingBlocks() = default;

    /// add `block_id` of query `id`. One query `id` can have several `blocks`
    /// return true if add successfully; otherwise return false;
    bool add(const String & id, UInt16 block_id);

    /// remove `block_id` of query `id`. One query `id` can have several `blocks`
    /// return true if remove successfully; otherwise return false;
    bool remove(const String & id, UInt16 block_id);

    /// ingest procgress calculation in percentage (0..100)
    /// return -1 if query `id` doesn't exist
    Int32 progress(const String & id) const;

    /// number of outstanding blocks
    size_t outstandingBlocks() const;

    /// set failure code for query `id`
    void fail(const String & id, UInt16 err);

    /// set the timeout for inflight blocks
    void setTimeout(Int32 timeout_sec_) { timeout_sec = timeout_sec_; }

private:
    void removeExpiredBlockIds();

private:
    using SteadyClock = std::chrono::time_point<std::chrono::steady_clock>;

    struct BlockIdInfo
    {
        UInt16 total = 0;
        UInt16 err = 0;
        std::unordered_set<UInt16> ids;
    };

private:
    mutable std::shared_mutex rwlock;
    std::unordered_map<String, BlockIdInfo> blockIds;

    std::mutex lock;
    std::deque<std::pair<SteadyClock, String>> timedBlockIds;

    Poco::Logger * log;
    Int32 timeout_sec = 120;
};
}
