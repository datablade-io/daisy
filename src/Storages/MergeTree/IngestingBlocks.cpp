#include "IngestingBlocks.h"

#include <common/logger_useful.h>

namespace DB
{
IngestingBlocks & IngestingBlocks::instance()
{
    static IngestingBlocks blocks;
    return blocks;
}

IngestingBlocks::IngestingBlocks() : log(&Poco::Logger::get("IngestingBlocks"))
{
}

bool IngestingBlocks::add(const String & id, UInt16 block_id)
{
    bool added = false;
    bool hasId = false;
    {
        std::unique_lock guard(rwlock);

        hasId = blockIds.find(id) != blockIds.end();

        auto & blockIdInfo = blockIds[id];
        const auto & res = blockIdInfo.ids.insert(block_id);
        if (res.second)
        {
            blockIdInfo.total++;
            added = true;
        }
    }

    if (!hasId)
    {
        auto now = std::chrono::steady_clock::now();
        /// if there are multiple blocks for an `id`
        /// only stamp the first block
        std::lock_guard guard(lock);
        timedBlockIds.emplace_back(now, id);
    }

    removeExpiredBlockIds();

    return added;
}

inline void IngestingBlocks::removeExpiredBlockIds()
{
    /// remove expired block ids
    std::vector<std::pair<String, size_t>> expired;
    {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard guard(lock);

        while (!timedBlockIds.empty())
        {
            const auto & p = timedBlockIds.front();

            if (std::chrono::duration_cast<std::chrono::seconds>(now - p.first).count() >= timeout_sec)
            {
                /// remove from timedBlockIds
                expired.push_back({});
                expired.back().first = p.second;

                timedBlockIds.pop_front();

                /// remove from blockIds
                std::unique_lock rwguard(rwlock);
                auto iter = blockIds.find(p.second);
                assert(iter != blockIds.end());

                expired.back().second = iter->second.ids.size();
                blockIds.erase(iter);
            }
            else
            {
                break;
            }
        }
    }

    for (const auto & p : expired)
    {
        LOG_WARNING(log, "query id={} timed out and there are {} remaing pending blocks waiting to be committed", p.first, p.second);
    }
}

bool IngestingBlocks::remove(const String & id, UInt16 block_id)
{
    std::unique_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        auto iterId = iter->second.ids.find(block_id);
        if (iterId != iter->second.ids.end())
        {
            iter->second.ids.erase(iterId);
            return true;
        }
    }
    return false;
}

Int32 IngestingBlocks::progress(const String & id) const
{
    std::shared_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        return (iter->second.total - iter->second.ids.size()) * 100 / iter->second.total;
    }
    return -1;
}

size_t IngestingBlocks::outstandingBlocks() const
{
    size_t total = 0;
    std::shared_lock guard(rwlock);
    for (const auto & p : blockIds)
    {
        total += p.second.ids.size();
    }
    return total;
}

void IngestingBlocks::fail(const String & id, UInt16 err)
{
    std::unique_lock guard(rwlock);
    auto iter = blockIds.find(id);
    if (iter != blockIds.end())
    {
        iter->second.err = err;
    }
}
}
