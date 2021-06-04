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
        Poco::Logger * log_,
        DWAL::WALPtr dwal_,
        const String & topic,
        Int32 shard);

    ~StreamingBlockInputStream() override;

    String getName() const override { return "StreamingBlockInputStream"; }
    Block getHeader() const override;

private:
    Block readImpl() override;
    void fetchRecordsFromStream();

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;

    size_t max_block_size;
    Poco::Logger * log;

    DWAL::WALPtr dwal;
    std::any consume_ctx;

    DWAL::RecordPtrs record_buffer;
    DWAL::RecordPtrs::iterator iter;
};
}
