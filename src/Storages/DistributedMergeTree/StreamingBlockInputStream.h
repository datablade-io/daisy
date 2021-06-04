#pragma once

#include "StorageDistributedMergeTree.h"

namespace DB
{

class StreamingBlockInputStream final : public IBlockInputStream
{
public:
    StreamingBlockInputStream(
        StorageDistributedMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names_,
        ContextPtr context_,
        size_t max_block_size_,
        Int32 shard_,
        Poco::Logger * log_);

    ~StreamingBlockInputStream() override;

    String getName() const override { return "StreamingBlockInputStream"; }
    Block getHeader() const override;

private:
    void readPrefixImpl() override;
    Block readImpl() override;
    void fetchRecordsFromStream();

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;

    size_t max_block_size;
    Int32 shard;
    Poco::Logger * log;

    String streaming_id;
    DWAL::WALPtr dwal;
    std::any consume_ctx;

    DWAL::RecordPtrs record_buffer;
    DWAL::RecordPtrs::iterator iter;
};
}
