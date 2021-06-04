#include "StreamingBlockInputStream.h"

#include <DistributedWriteAheadLog/KafkaWAL.h>
#include <DistributedWriteAheadLog/Name.h>
#include <DistributedWriteAheadLog/WALPool.h>

namespace DB
{
StreamingBlockInputStream::StreamingBlockInputStream(
    StorageDistributedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & column_names_,
    ContextPtr context_,
    size_t max_block_size_,
    Int32 shard_,
    Poco::Logger * log_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(column_names_)
    , max_block_size(max_block_size_)
    , shard(shard_)
    , log(log_)
{
}

StreamingBlockInputStream::~StreamingBlockInputStream()
{
    if (dwal)
    {
        dwal->stopConsume(consume_ctx);
        DWAL::WALPool::instance(context).deleteStreaming(streaming_id);
        dwal = nullptr;
    }
}

Block StreamingBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

void StreamingBlockInputStream::readPrefixImpl()
{
    assert(!consume_ctx.has_value());
    assert(!dwal);

    const auto & storage_id = storage.getStorageID();
    auto topic = DWAL::escapeDWalName(storage_id.getDatabaseName(), storage_id.getTableName());
    DWAL::KafkaWALContext kctx{topic, shard, Int64{-1} /* latest */};
    kctx.auto_offset_reset = "latest";

    consume_ctx = std::move(kctx);
    iter = record_buffer.begin();

    /// FIXME, make sure unique streaming ID
    streaming_id = topic + "-" + context->getCurrentQueryId();
    dwal = DWAL::WALPool::instance(context).getOrCreateStreaming(streaming_id, storage.streamingStorageClusterId());
    assert(dwal);

    LOG_INFO(log, "Streaming reading from table={} shard={}", topic, shard);
}

Block StreamingBlockInputStream::readImpl()
{
    assert(dwal);
    assert(consume_ctx.has_value());

    if (isCancelled())
    {
        return Block{};
    }

    if (record_buffer.empty() || iter == record_buffer.end())
    {
        /// Fetch more from streaming storage
        fetchRecordsFromStream();
    }

    if (record_buffer.empty())
    {
        /// Act as a heart beat
        return getHeader();
    }

    auto & r = *iter;
    ++iter;
    return std::move(r->block);
}

void StreamingBlockInputStream::fetchRecordsFromStream()
{
    assert(record_buffer.empty() || iter == record_buffer.end());

    (void)max_block_size;

    DWAL::WAL::ConsumeResult result{dwal->consume(100, 1000, consume_ctx)};
    if (result.err != 0)
    {
        LOG_ERROR(log, "Failed to consume stream, err={}", result.err);
        return;
    }

    record_buffer.swap(result.records);

    /// Reset `iter`
    iter = record_buffer.begin();
}
}
