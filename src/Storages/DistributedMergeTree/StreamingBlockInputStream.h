#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class StorageDistributedMergeTree;
class StreamingBlockReader;

class StreamingBlockInputStream final : public IBlockInputStream
{
public:
    StreamingBlockInputStream(
        StorageDistributedMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names_,
        ContextPtr context_,
        Int32 shard_,
        Poco::Logger * log_);

    ~StreamingBlockInputStream() override;

    String getName() const override { return "StreamingBlockInputStream"; }
    Block getHeader() const override;

private:
    void readPrefixImpl() override;
    Block readImpl() override;
    void readAndProcess();

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;

    Int32 shard;
    Poco::Logger * log;

    std::unique_ptr<StreamingBlockReader> reader;

    /// std::condition_variable fire_condition;
    /// std::mutex result_blocks_mutex;

    /// Final results
    Blocks result_blocks;
    Blocks::iterator iter;
};
}
