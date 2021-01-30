#include "StorageDistributedMergeTree.h"

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>

#include <Processors/Pipe.h>
#include <Storages/MergeTree/DistributedMergeTreeBlockOutputStream.h>
#include <Common/Macros.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
String getDefaultClientId(const StorageID & table_id_)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id_.database_name, table_id_.table_name);
}

ExpressionActionsPtr
buildShardingKeyExpression(const ASTPtr & sharding_key, const Context & context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

bool isExpressionActionsDeterministics(const ExpressionActionsPtr & actions)
{
    for (const auto & action : actions->getActions())
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
            continue;
        if (!action.node->function_base->isDeterministic())
            return false;
    }
    return true;
}
}


StorageDistributedMergeTree::StorageDistributedMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    size_t shard_count_,
    size_t replication_factor_,
    const ASTPtr & sharding_key_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    std::unique_ptr<SystemKafkaSettings> system_kafka_settings_,
    bool /*has_force_restore_data_flag*/)
    : MergeTreeData(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(storage_settings_),
        false, /// require_part_metadata
        attach)
    , system_kafka_settings(std::move(system_kafka_settings_))
    , brokers(global_context.getMacros()->expand(system_kafka_settings->kafka_broker_list.value))
    , topic(global_context.getMacros()->expand(system_kafka_settings->kafka_topic.value))
    , client_id(getDefaultClientId(table_id_))
    , partition_id(system_kafka_settings->kafka_partition.value)
    , num_consumers(system_kafka_settings->kafka_num_consumers.value)
    , log(&Poco::Logger::get("StorageDistributedMergeTree (" + table_id_.table_name + ")"))
    , has_sharding_key(sharding_key_)
    , shard_count(shard_count_)
    , replication_factor(replication_factor_)
    , slot_to_shard(shard_count, 1)
    , rng(randomSeed())
{
    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, global_context, metadata_.getColumns().getAllPhysical(), false);
        sharding_key_is_deterministic = isExpressionActionsDeterministics(sharding_key_expr);
        sharding_key_column_name = sharding_key_->getColumnName();
    }
}

Pipe StorageDistributedMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    (void)column_names;
    (void)metadata_snapshot;
    (void)context;
    (void)max_block_size;
    (void)num_streams;
    return {};
}

BlockOutputStreamPtr
StorageDistributedMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    const auto & settings = context.getSettingsRef();
    return std::make_shared<DistributedMergeTreeBlockOutputStream>(
        *this, metadata_snapshot, settings.max_partitions_per_insert_block, context.getSettingsRef().optimize_on_insert);
}

void StorageDistributedMergeTree::startup()
{
}

void StorageDistributedMergeTree::shutdown()
{
}

String StorageDistributedMergeTree::getName() const
{
    return "StorageDistributedMergeTree";
}

bool StorageDistributedMergeTree::supportsParallelInsert() const
{
    return true;
}

bool StorageDistributedMergeTree::supportsIndexForIn() const
{
    return true;
}

std::optional<UInt64> StorageDistributedMergeTree::totalRows(const Settings &) const
{
    return {};
}

std::optional<UInt64> StorageDistributedMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo &, const Context &) const
{
    return {};
}

std::optional<UInt64> StorageDistributedMergeTree::totalBytes(const Settings &) const
{
    return {};
}

NamesAndTypesList StorageDistributedMergeTree::getVirtuals() const
{
    return {};
}

bool StorageDistributedMergeTree::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool finall,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    const Context & context)
{
    (void)query;
    (void)partition;
    (void)finall;
    (void)deduplicate;
    (void)deduplicate_by_columns;
    (void)context;
    return false;
}

void StorageDistributedMergeTree::mutate(const MutationCommands & commands, const Context & context)
{
    (void)commands;
    (void)context;
}

/// Return introspection information about currently processing or recently processed mutations.
std::vector<MergeTreeMutationStatus> StorageDistributedMergeTree::getMutationsStatus() const
{
    return {};
}

CancellationCode StorageDistributedMergeTree::killMutation(const String & mutation_id)
{
    (void)mutation_id;
    return {};
}

void StorageDistributedMergeTree::drop()
{
}

void StorageDistributedMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
}


void StorageDistributedMergeTree::alter(const AlterCommands & commands, const Context & context, TableLockHolder & table_lock_holder)
{
    (void)commands;
    (void)context;
    (void)table_lock_holder;
}

void StorageDistributedMergeTree::checkTableCanBeDropped() const
{
}

ActionLock StorageDistributedMergeTree::getActionLock(StorageActionBlockType action_type)
{
    (void)action_type;
    return {};
}

void StorageDistributedMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    (void)action_type;
}

CheckResults StorageDistributedMergeTree::checkData(const ASTPtr & query, const Context & context)
{
    (void)query;
    (void)context;
    return {};
}

std::optional<JobAndPool> StorageDistributedMergeTree::getDataProcessingJob()
{
    return {};
}

MergeTreeDataPartType StorageDistributedMergeTree::choosePartType(size_t /*bytes_uncompressed*/, size_t /*rows_count*/) const
{
    return MergeTreeDataPartType::DISTRIBUTED;
}

IColumn::Selector StorageDistributedMergeTree::createSelector(const ColumnWithTypeAndName & result) const
{
// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE)                                                                                       \
    if (typeid_cast<const DataType##TYPE *>(result.type.get()))                                                     \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard);                                            \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result.type.get()))          \
        if (typeid_cast<const DataType ## TYPE *>(type_low_cardinality->getDictionaryType().get()))                 \
            return createBlockSelector<TYPE>(*result.column->convertToFullColumnIfLowCardinality(), slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}


IColumn::Selector StorageDistributedMergeTree::createSelector(const Block & block) const
{
    Block current_block_with_sharding_key_expr = block;
    sharding_key_expr->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(sharding_key_column_name);

    return createSelector(key_column);
}

const ExpressionActionsPtr & StorageDistributedMergeTree::getShardingKeyExpr() const
{
    return sharding_key_expr;
}

const String & StorageDistributedMergeTree::getShardingKeyColumnName() const
{
    return sharding_key_column_name;
}

size_t StorageDistributedMergeTree::getShardCount() const
{
    return shard_count;
}

size_t StorageDistributedMergeTree::getRandomShardIndex()
{
    std::lock_guard lock(rng_mutex);
    return std::uniform_int_distribution<size_t>(0, shard_count - 1)(rng);
}
}
