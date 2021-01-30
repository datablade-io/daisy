#include <Storages/MergeTree/DistributedMergeTreeBlockOutputStream.h>

#include <Interpreters/PartLog.h>
#include <Storages/StorageDistributedMergeTree.h>


namespace DB
{
Block DistributedMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

BlocksWithShard DistributedMergeTreeBlockOutputStream::doShardBlock(const Block & block) const
{
    auto selector = storage.createSelector(block);

    Blocks sharded_blocks(storage.shard_count);

    for (size_t shard_idx = 0; shard_idx < storage.shard_count; ++shard_idx)
        sharded_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns sharded_columns = block.getByPosition(col_idx_in_block).column->scatter(storage.shard_count, selector);
        for (size_t shard_idx = 0; shard_idx < storage.shard_count; ++shard_idx)
            sharded_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(sharded_columns[shard_idx]);
    }

    BlocksWithShard blocks_with_shard;

    for (size_t shard_idx = 0; shard_idx < sharded_blocks.size(); ++shard_idx)
        if (sharded_blocks[shard_idx].rows())
            blocks_with_shard.emplace_back(std::move(sharded_blocks[shard_idx]), shard_idx);

    return blocks_with_shard;
}

BlocksWithShard DistributedMergeTreeBlockOutputStream::shardBlock(const Block & block) const
{
    size_t shard = 0;
    if (storage.shard_count > 1)
    {
        if (storage.has_sharding_key)
        {
            return doShardBlock(block);
        }
        else
        {
            /// randomly pick one shard to ingest this block
            shard = storage.getRandomShardIndex();
        }
    }

    return {BlockWithShard{Block(block), shard}};
}

void DistributedMergeTreeBlockOutputStream::write(const Block & block)
{
    /// 1) Split block by sharding key
    BlocksWithShard blocks{shardBlock(block)};

    /// 2) Commit each sharded block to corresponding Kafka partition
    for (auto & current_block : blocks)
    {
        (void)current_block;
    }
}
}
