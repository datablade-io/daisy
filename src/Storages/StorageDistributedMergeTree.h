#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/StorageMergeTree.h>

#include <pcg_random.hpp>

namespace DB
{

/// FIXME, move the Kafka settings class in a correct place.

#define SYSTEM_KAFKA_RELATED_SETTINGS(M) \
    M(String, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.", 0) \
    M(String, kafka_topic, "", "Kafka topic.", 0) \
    M(Int64, kafka_partition, -1, "Kafka partition to poll from.", 0) \
    M(UInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.", 0) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, kafka_poll_timeout_ms, 0, "Timeout for single poll from Kafka.", 0) \
    /* default is min(max_block_size, kafka_max_block_size)*/ \
    M(UInt64, kafka_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    /* default is = max_insert_block_size / kafka_num_consumers  */ \
    M(UInt64, kafka_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0)

/** TODO: */
/* https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md */
/* https://github.com/edenhill/librdkafka/blob/v1.4.2/src/rdkafka_conf.c */

#define LIST_OF_SYSTEM_KAFKA_SETTINGS(M) \
    SYSTEM_KAFKA_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(SystemKafkaSettingsTraits, LIST_OF_SYSTEM_KAFKA_SETTINGS)


struct SystemKafkaSettings : public BaseSettings<SystemKafkaSettingsTraits>
{
};

/** See the description of the data structure in MergeTreeData.
  */
class StorageDistributedMergeTree final : public ext::shared_ptr_helper<StorageDistributedMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageDistributedMergeTree>;
public:
    ~StorageDistributedMergeTree() override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    void startup() override;
    void shutdown() override;

    String getName() const override;
    bool supportsParallelInsert() const override;
    bool supportsIndexForIn() const override;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, const Context &) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;
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

    MergeTreeDataPartType choosePartType(size_t bytes_uncompressed, size_t rows_count) const override;

    IColumn::Selector createSelector(const ColumnWithTypeAndName & result) const;
    IColumn::Selector createSelector(const Block & block) const;

    const ExpressionActionsPtr & getShardingKeyExpr() const;

    const String & getShardingKeyColumnName() const;

    size_t getShardCount() const;

    size_t getRandomShardIndex();

    friend class DistributedMergeTreeBlockOutputStream;
    friend class MergeTreeData;

protected:
    StorageDistributedMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        size_t shard_count_,
        size_t replication_factor_,
        const ASTPtr & sharding_key_,
        std::unique_ptr<MergeTreeSettings> settings_,
        std::unique_ptr<SystemKafkaSettings> system_kafka_settings_,
        bool has_force_restore_data_flag);

private:
    /// Kafka Settings
    std::unique_ptr<SystemKafkaSettings> system_kafka_settings;
    const String brokers;
    const String topic;

    /// For Kafka consumer
    String client_id;
    Int64 partition_id;
    UInt64 num_consumers;

    Poco::Logger * log;

    /// For block numbers.
    SimpleIncrement increment;

    /// For sharding
    bool has_sharding_key;
    bool sharding_key_is_deterministic = false;
    size_t shard_count;
    size_t replication_factor;
    std::vector<UInt64> slot_to_shard;
    ExpressionActionsPtr sharding_key_expr;
    String sharding_key_column_name;

    // For random shard index generation
    mutable std::mutex rng_mutex;
    pcg64 rng;
};
}
