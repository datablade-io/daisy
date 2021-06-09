#include "StreamingBlockInputStream.h"

#include "StorageDistributedMergeTree.h"
#include "StreamingBlockReader.h"

namespace DB
{
StreamingBlockInputStream::StreamingBlockInputStream(
    StorageDistributedMergeTree & storage_,
    ASTPtr query_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & column_names_,
    ContextPtr context_,
    Int32 shard_,
    Poco::Logger * log_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query(query_)
    , context(context_)
    , column_names(column_names_)
    , shard(shard_)
    , log(log_)
{
}

StreamingBlockInputStream::~StreamingBlockInputStream()
{
}

Block StreamingBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

void StreamingBlockInputStream::readPrefixImpl()
{
    assert(!reader);

    reader = std::make_unique<StreamingBlockReader>(storage, context, shard, log);
    iter = result_blocks.begin();
}

Block StreamingBlockInputStream::readImpl()
{
    assert(reader);

    if (isCancelled())
    {
        return {};
    }

    /// std::unique_lock lock(result_blocks_mutex);
    if (result_blocks.empty() || iter == result_blocks.end())
    {
        readAndProcess();

        if (isCancelled())
        {
            return {};
        }

        /// After processing blocks, check again to see if there are new results
        if (result_blocks.empty() || iter == result_blocks.end())
        {
            /// Act as a heart beat
            return getHeader();
        }

        /// result_blocks is not empty, fallthrough
    }

    return std::move(*iter++);
}


void StreamingBlockInputStream::readAndProcess()
{
    /// fire_condition.wait_for(lock, std::chrono::seconds(2));

    /// 1 seconds heartbeat interval
    auto records = reader->read(1000);
    if (records.empty())
    {
        return;
    }

    /// 1) Insert raw blocks to in-memory aggregation table
    /// 2) Select the final result from the aggregated table
    /// 3) Update reesult_blocks and iterator
    result_blocks.clear();
    result_blocks.reserve(records.size());

    /// For now, just use the raw blocks
    for (auto & record : records)
    {
        result_blocks.push_back(std::move(record->block));
    }
    iter = result_blocks.begin();
}
}
