#pragma once

#include <pcg_random.hpp>
#include <ext/shared_ptr_helper.h>

#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Storages/MergeTree/IngestingBlocks.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>

#include <Common/ThreadPool.h>


namespace DB
{

class StorageMergeTree;

/** See the description of the data structure in MergeTreeData.
  */
class StorageDistributedMergeTree final : public ext::shared_ptr_helper<StorageDistributedMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageDistributedMergeTree>;
public:
    void startup() override;
    void shutdown() override;
    ~StorageDistributedMergeTree() override = default;

    String getName() const override;

    bool supportsParallelInsert() const override;

    bool supportsIndexForIn() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, const Context &) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    NamesAndTypesList getVirtuals() const override;

    /** Perform the next step in combining the parts.
      */
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    /// Return introspection information about currently processing or recently processed mutations.
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;

    CancellationCode killMutation(const String & mutation_id) override;

    void drop() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    void alter(const AlterCommands & commands, const Context & context, TableLockHolder & table_lock_holder) override;

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    CheckResults checkData(const ASTPtr & query, const Context & context) override;

    std::optional<JobAndPool> getDataProcessingJob() override;

private:
    /// Partition helpers

    void dropPartition(const ASTPtr & partition, bool detach, bool drop_part, const Context & context, bool throw_if_noop = true) override;

    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, const Context & context) override;

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context) override;

    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & context) override;

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overridden by children, because they can have different
    /// mechanisms for parts locking
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    /// Return most recent mutations commands for part which weren't applied
    /// Used to receive AlterConversions for part and apply them on fly. This
    /// method has different implementations for replicated and non replicated
    /// MergeTree because they store mutations in different way.
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    void startBackgroundMovesIfNeeded() override;

public:
    IColumn::Selector createSelector(const ColumnWithTypeAndName & result) const;
    IColumn::Selector createSelector(const Block & block) const;

    const ExpressionActionsPtr & getShardingKeyExpr() const;

    const String & getShardingKeyColumnName() const { return sharding_key_column_name; }

    Int32 getShards() const { return shards; }
    Int32 getReplicationFactor() const { return replication_factor; }

    size_t getRandomShardIndex();

    friend class DistributedMergeTreeBlockOutputStream;
    friend class MergeTreeData;

protected:
    StorageDistributedMergeTree(
        Int32 replication_factor_,
        Int32 shards_,
        const ASTPtr & sharding_key_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach_,
        Context & context_,
        const String & date_column_name_,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag_);

private:
    void initWal();
    IDistributedWriteAheadLog::RecordSequenceNumber lastSequenceNumber();

private:
    struct WriteCallbackData
    {
        String query_status_poll_id;
        UInt16 block_id;
        StorageDistributedMergeTree * storage;

        WriteCallbackData(const String & query_status_poll_id_, UInt16 block_id_, StorageDistributedMergeTree * storage_)
            : query_status_poll_id(query_status_poll_id_), block_id(block_id_), storage(storage_)
        {
        }
    };

    WriteCallbackData * writeCallbackData(const String & query_status_poll_id, UInt16 block_id);
    void writeCallback(const IDistributedWriteAheadLog::AppendResult & result, const String & query_status_poll_id, UInt16 block_id);

    static void writeCallback(const IDistributedWriteAheadLog::AppendResult & result, void * data);

    void backgroundConsumer();
    void mergeBlocks(Block & lhs, Block & rhs);
    void
    commit(const std::vector<IDistributedWriteAheadLog::RecordPtrs> & records, Block & block, Int64 & last_sn, std::any & dwal_consume_ctx);
    void doCommit(Block & block, Int64 & last_sn, std::any & dwal_consume_ctx);

    std::any & getDwalAppendCtx();

private:
    Int32 replication_factor;
    Int32 shards;
    ExpressionActionsPtr sharding_key_expr;

    /// From table settings for producer
    Int32 dwal_request_timeout_ms = 30000;
    Int32 dwal_request_required_acks = 1;

    /// From table settings for consumer
    String dwal_auto_offset_reset = "earliest";
    Int32 dwal_partition = -1;

    /// For sharding
    bool sharding_key_is_deterministic = false;
    std::vector<UInt64> slot_to_shard;
    String sharding_key_column_name;

    /// cached ctx for reuse
    std::mutex append_ctx_mutex;
    std::any dwal_append_ctx;

    IDistributedWriteAheadLog::RecordSequenceNumber dwal_last_sn = -1;

    DistributedWriteAheadLogPtr dwal;
    IngestingBlocks & ingesting_blocks;
    std::optional<ThreadPool> tailer;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;

    /// forwarding storage if it is not virtual
    std::shared_ptr<StorageMergeTree> storage;

    std::atomic_flag stopped;
};
}
