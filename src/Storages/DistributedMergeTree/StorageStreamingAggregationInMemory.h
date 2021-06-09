#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageStreamingAggregationInMemory final : public ext::shared_ptr_helper<StorageStreamingAggregationInMemory>, public IStorage
{
public:
    String getName() const override { return "StorageStreamingAggregationInMemory"; }

    void startup() override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /* metadata_snapshot */,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// Smaller blocks (e.g. 64K rows) are better for CPU cache.
    bool prefersLargeBlocks() const override { return false;  }

    bool noPushingToViews() const override { return true;  }

private:
    ContextPtr query_context;
    Poco::Logger * log;

protected:
    StorageStreamingAggregationInMemory(const StorageID & storage_id_, const ASTPtr & select_query, ContextPtr query_context_, Poco::Logger * log);
};
}
