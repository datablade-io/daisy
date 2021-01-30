#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{
class Block;
class StorageDistributedMergeTree;

struct BlockWithShard
{
    Block block;
    size_t shard;

    BlockWithShard(Block && block_, size_t shard_) : block(block_), shard(shard_) { }
};

using BlocksWithShard = std::vector<BlockWithShard>;

class DistributedMergeTreeBlockOutputStream final : public IBlockOutputStream
{
public:
    DistributedMergeTreeBlockOutputStream(
        StorageDistributedMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, size_t max_parts_per_block_, bool optimize_on_insert_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_parts_per_block(max_parts_per_block_)
        , optimize_on_insert(optimize_on_insert_)
    {
    }

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    BlocksWithShard shardBlock(const Block & block) const;
    BlocksWithShard doShardBlock(const Block & block) const;

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    bool optimize_on_insert;
};

}
