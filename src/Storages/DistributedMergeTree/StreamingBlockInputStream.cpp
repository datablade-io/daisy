#include "StreamingBlockInputStream.h"

#include <DistributedWriteAheadLog/KafkaWAL.h>

namespace DB
{
StreamingBlockInputStream::StreamingBlockInputStream(
    StorageDistributedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & column_names_,
    ContextPtr context_,
    size_t max_block_size_,
    Poco::Logger * log_,
    DWAL::WALPtr dwal_,
    const String & topic,
    Int32 shard)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(column_names_)
    , max_block_size(max_block_size_)
    , log(log_)
    , dwal(dwal_)
{
    DWAL::KafkaWALContext kctx{topic, shard, -1};
    kctx.auto_offset_reset = "latest";

    consume_ctx = std::move(kctx);

    iter = record_buffer.begin();
}

StreamingBlockInputStream::~StreamingBlockInputStream()
{
}

Block StreamingBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

Block StreamingBlockInputStream::readImpl()
{
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

    return (*iter)->block;
}

void StreamingBlockInputStream::fetchRecordsFromStream()
{
    assert(record_buffer.empty() || iter == record_buffer.end());

    (void)max_block_size;

    DWAL::WAL::ConsumeResult result{dwal->consume(100, 500, consume_ctx)};
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
