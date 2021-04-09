#include "StorageDistributedMergeTree.h"

#include <DistributedMetadata/CatalogService.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Functions/IFunction.h>
#include <Interpreters/ClusterProxy/DistributedSelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/DistributedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Common/randomSeed.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NOT_IMPLEMENTED;
    extern const int OK;
    extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
    extern const int TOO_MANY_ROWS;
}

namespace
{
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY = 1;
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS = 2;
const UInt64 DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION = 2;

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

class ReplacingConstantExpressionsMatcher
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            auto result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            node = std::make_shared<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
        }
    }
};

void replaceConstantExpressions(
    ASTPtr & node,
    const Context & context,
    const NamesAndTypesList & columns,
    ConstStoragePtr storage,
    const StorageMetadataPtr & metadata_snapshot)
{
    auto syntax_result = TreeRewriter(context).analyze(node, columns, storage, metadata_snapshot);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(block_with_constants);
    visitor.visit(node);
}

/// Returns one of the following:
/// - QueryProcessingStage::Complete
/// - QueryProcessingStage::WithMergeableStateAfterAggregation
/// - none (in this case regular WithMergeableState should be used)
std::optional<QueryProcessingStage::Enum> getOptimizedQueryProcessingStage(const ASTPtr & query_ptr, bool extremes, const Block & sharding_key_block)
{
    const auto & select = query_ptr->as<ASTSelectQuery &>();

    auto sharding_block_has = [&](const auto & exprs, size_t limit = SIZE_MAX) -> bool
    {
        size_t i = 0;
        for (auto & expr : exprs)
        {
            ++i;
            if (i > limit)
                break;

            auto id = expr->template as<ASTIdentifier>();
            if (!id)
                return false;
            /// TODO: if GROUP BY contains multiIf()/if() it should contain only columns from sharding_key
            if (!sharding_key_block.has(id->name()))
                return false;
        }
        return true;
    };

    // GROUP BY qualifiers
    // - TODO: WITH TOTALS can be implemented
    // - TODO: WITH ROLLUP can be implemented (I guess)
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube)
        return {};

    // TODO: extremes support can be implemented
    if (extremes)
        return {};

    // DISTINCT
    if (select.distinct)
    {
        if (!sharding_block_has(select.select()->children))
            return {};
    }

    // GROUP BY
    const ASTPtr group_by = select.groupBy();
    if (!group_by)
    {
        if (!select.distinct)
            return {};
    }
    else
    {
        if (!sharding_block_has(group_by->children, 1))
            return {};
    }

    // ORDER BY
    const ASTPtr order_by = select.orderBy();
    if (order_by)
        return QueryProcessingStage::WithMergeableStateAfterAggregation;

    // LIMIT BY
    // LIMIT
    // OFFSET
    if (select.limitBy() || select.limitLength() || select.limitOffset())
        return QueryProcessingStage::WithMergeableStateAfterAggregation;

    // Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

size_t getClusterQueriedNodes(const Settings & settings, const ClusterPtr & cluster)
{
    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    return (num_remote_shards * settings.max_parallel_replicas) + num_local_shards;
}

std::string makeFormattedListOfShards(const ClusterPtr & cluster)
{
    WriteBufferFromOwnString buf;

    bool head = true;
    buf << "[";
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        (head ? buf : buf << ", ") << shard_info.shard_num;
        head = false;
    }
    buf << "]";

    return buf.str();
}
}

StorageDistributedMergeTree::StorageDistributedMergeTree(
    Int32 replication_factor_,
    Int32 shards_,
    const ASTPtr & sharding_key_,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach_,
    Context & context_,
    const String & date_column_name_,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool has_force_restore_data_flag_)
    : MergeTreeData(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::make_unique<MergeTreeSettings>(*settings_.get()), /// make a copy
        false, /// require_part_metadata
        attach_)
    , replication_factor(replication_factor_)
    , shards(shards_)
    , ingesting_blocks(IngestingBlocks::instance())
    , part_commit_pool(context_.getPartCommitPool())
    , rng(randomSeed())
{
    if (!relative_data_path_.empty())
    {
        /// Virtual table which is for data ingestion only
        storage = StorageMergeTree::create(
            table_id_,
            relative_data_path_,
            metadata_,
            attach_,
            context_,
            date_column_name_,
            merging_params_,
            std::move(settings_),
            has_force_restore_data_flag_);
        tailer.emplace(1);

        /// Load sn and setup it
        auto sn = storage->loadSN();
        storage->setCommittedSN(sn);

        if (sn >= 0)
        {
            std::lock_guard lock(sns_mutex);
            last_sn = sn;
            local_sn = sn;
        }
        LOG_INFO(log, "Load committed sequence={}", sn);
    }

    initWal();

    for (Int32 shardId = 0; shardId < shards; ++shardId)
    {
        slot_to_shard.push_back(shardId);
    }

    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, global_context, metadata_.getColumns().getAllPhysical(), false);
        sharding_key_is_deterministic = isExpressionActionsDeterministics(sharding_key_expr);
        sharding_key_column_name = sharding_key_->getColumnName();
    }
}

void StorageDistributedMergeTree::readRemote(
    QueryPlan & query_plan, SelectQueryInfo & query_info, const Context & context, QueryProcessingStage::Enum processed_stage)
{
    Block header = InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage)).getSampleBlock();
    const Scalars & scalars = context.hasQueryContext() ? context.getQueryContext().getScalars() : Scalars{};

    ClusterProxy::DistributedSelectStreamFactory select_stream_factory
        = ClusterProxy::DistributedSelectStreamFactory(header, processed_stage, getStorageID(), scalars, context.getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, query_info.query, context, query_info);
}

void StorageDistributedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    if (isRemote())
    {
        /// This is a distributed query
        readRemote(query_plan, query_info, context, processed_stage);
    }
    else
    {
        storage->read(query_plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }
}

Pipe StorageDistributedMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
}

void StorageDistributedMergeTree::startup()
{
    if (storage)
    {
        storage->startup();
        tailer->scheduleOrThrowOnError([this] { backgroundConsumer(); });
    }
}

void StorageDistributedMergeTree::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    if (storage)
    {
        tailer->wait();
        storage->shutdown();
    }
    LOG_INFO(log, "Stopped");
}

String StorageDistributedMergeTree::getName() const
{
    return "DistributedMergeTree";
}

bool StorageDistributedMergeTree::isRemote() const
{
    /// If it is virtual table, then it is remote
    /// virtual table doesn't have backing storage
    return !storage;
}

bool StorageDistributedMergeTree::supportsParallelInsert() const
{
    return true;
}

bool StorageDistributedMergeTree::supportsIndexForIn() const
{
    return true;
}

std::optional<UInt64> StorageDistributedMergeTree::totalRows(const Settings & settings) const
{
    return storage->totalRows(settings);
}

std::optional<UInt64>
StorageDistributedMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, const Context & context) const
{
    return storage->totalRowsByPartitionPredicate(query_info, context);
}

std::optional<UInt64> StorageDistributedMergeTree::totalBytes(const Settings & settings) const
{
    return storage->totalBytes(settings);
}

BlockOutputStreamPtr
StorageDistributedMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    return std::make_shared<DistributedMergeTreeBlockOutputStream>(*this, metadata_snapshot, context);
}

void StorageDistributedMergeTree::checkTableCanBeDropped() const
{
    storage->checkTableCanBeDropped();
}

void StorageDistributedMergeTree::drop()
{
    /// FIXME : remove kafka topic, tear down tail thread
    storage->drop();
}

void StorageDistributedMergeTree::truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const Context & context, TableExclusiveLockHolder & holder)
{
    storage->truncate(query, metadata_snapshot, context, holder);
}

void StorageDistributedMergeTree::alter(const AlterCommands & commands, const Context & context, TableLockHolder & table_lock_holder)
{
    storage->alter(commands, context, table_lock_holder);
}

NamesAndTypesList StorageDistributedMergeTree::getVirtuals() const
{
    return storage->getVirtuals();
}

bool StorageDistributedMergeTree::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool finall,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    const Context & context)
{
    return storage->optimize(query, metadata_snapshot, partition, finall, deduplicate, deduplicate_by_columns, context);
}

void StorageDistributedMergeTree::mutate(const MutationCommands & commands, const Context & context)
{
    storage->mutate(commands, context);
}

/// Return introspection information about currently processing or recently processed mutations.
std::vector<MergeTreeMutationStatus> StorageDistributedMergeTree::getMutationsStatus() const
{
    return storage->getMutationsStatus();
}

CancellationCode StorageDistributedMergeTree::killMutation(const String & mutation_id)
{
    return storage->killMutation(mutation_id);
}

ActionLock StorageDistributedMergeTree::getActionLock(StorageActionBlockType action_type)
{
    return storage->getActionLock(action_type);
}

void StorageDistributedMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    storage->onActionLockRemove(action_type);
}

CheckResults StorageDistributedMergeTree::checkData(const ASTPtr & query, const Context & context)
{
    return storage->checkData(query, context);
}

std::optional<JobAndPool> StorageDistributedMergeTree::getDataProcessingJob()
{
    return storage->getDataProcessingJob();
}

QueryProcessingStage::Enum StorageDistributedMergeTree::getQueryProcessingStage(
    const Context & context, QueryProcessingStage::Enum to_stage, SelectQueryInfo & query_info) const
{
    if (isRemote())
    {
        return getQueryProcessingStageRemote(context, to_stage, query_info);
    }
    else
    {
        return storage->getQueryProcessingStage(context, to_stage, query_info);
    }
}

void StorageDistributedMergeTree::dropPartition(const ASTPtr & partition, bool detach, bool drop_part, const Context & context, bool throw_if_noop)
{
    storage->dropPartition(partition, detach, drop_part, context, throw_if_noop);
}

PartitionCommandsResultInfo StorageDistributedMergeTree::attachPartition(
    const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, const Context & context)
{
    return storage->attachPartition(partition, metadata_snapshot, part, context);
}

void StorageDistributedMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    storage->replacePartitionFrom(source_table, partition, replace, context);
}

void StorageDistributedMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & context)
{
    storage->movePartitionToTable(dest_table, partition, context);
}

/// If part is assigned to merge or mutation (possibly replicated)
/// Should be overridden by children, because they can have different
/// mechanisms for parts locking
bool StorageDistributedMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    return storage->partIsAssignedToBackgroundOperation(part);
}

/// Return most recent mutations commands for part which weren't applied
/// Used to receive AlterConversions for part and apply them on fly. This
/// method has different implementations for replicated and non replicated
/// MergeTree because they store mutations in different way.
MutationCommands StorageDistributedMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    return storage->getFirstAlterMutationCommandsForPart(part);
}

void StorageDistributedMergeTree::startBackgroundMovesIfNeeded()
{
    return storage->startBackgroundMovesIfNeeded();
}

/// Distributed query related functions

ClusterPtr StorageDistributedMergeTree::getCluster() const
{
    auto sid = getStorageID();
    return CatalogService::instance(global_context).tableCluster(sid.database_name, sid.table_name, replication_factor, shards);
}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageDistributedMergeTree::skipUnusedShards(
    ClusterPtr cluster, const ASTPtr & query_ptr, const StorageMetadataPtr & metadata_snapshot, const Context & context) const
{
    const auto & select = query_ptr->as<ASTSelectQuery &>();

    if (!select.prewhere() && !select.where())
    {
        return nullptr;
    }

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
    {
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    }
    else
    {
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();
    }

    replaceConstantExpressions(condition_ast, context, metadata_snapshot->getColumns().getAll(), shared_from_this(), metadata_snapshot);
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr);

    /// Can't get definite answer if we can skip any shards
    if (!blocks)
    {
        return nullptr;
    }

    std::set<int> shard_ids;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
        {
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TOO_MANY_ROWS);
        }

        const ColumnWithTypeAndName & result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(result);

        shard_ids.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shard_ids.begin(), shard_ids.end()});
}

ClusterPtr StorageDistributedMergeTree::getOptimizedCluster(
    const Context & context, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const
{
    ClusterPtr cluster = getCluster();
    const Settings & settings = context.getSettingsRef();

    bool sharding_key_is_usable = settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic;

    if (sharding_key_expr && sharding_key_is_usable)
    {
        ClusterPtr optimized = skipUnusedShards(cluster, query_ptr, metadata_snapshot, context);
        if (optimized)
        {
            return optimized;
        }
    }

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (force)
    {
        WriteBufferFromOwnString exception_message;
        if (!sharding_key_expr)
        {
            exception_message << "No sharding key";
        }
        else if (!sharding_key_is_usable)
        {
            exception_message << "Sharding key is not deterministic";
        }
        else
        {
            exception_message << "Sharding key " << sharding_key_column_name << " is not used";
        }

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS)
        {
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
        }

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && sharding_key_expr)
        {
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
        }
    }

    return cluster;
}

QueryProcessingStage::Enum StorageDistributedMergeTree::getQueryProcessingStageRemote(
    const Context & context, QueryProcessingStage::Enum to_stage, SelectQueryInfo & query_info) const
{
    const auto & settings = context.getSettingsRef();

    ClusterPtr cluster = getCluster();
    query_info.cluster = cluster;

    /// Always calculate optimized cluster here to avoid conditions during read()
    if (settings.optimize_skip_unused_shards)
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        ClusterPtr optimized_cluster = getOptimizedCluster(context, metadata_snapshot, query_info.query);
        if (optimized_cluster)
        {
            LOG_DEBUG(log, "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): {}", makeFormattedListOfShards(optimized_cluster));
            cluster = optimized_cluster;
            query_info.cluster = cluster;
        }
        else
        {
            LOG_DEBUG(
                log,
                "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - the query will be sent to all shards of the "
                "cluster{}",
                sharding_key_expr ? "" : " (no sharding key)");
        }
    }

    if (settings.distributed_group_by_no_merge)
    {
        if (settings.distributed_group_by_no_merge == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION)
            return QueryProcessingStage::WithMergeableStateAfterAggregation;
        else
            return QueryProcessingStage::Complete;
    }

    /// Nested distributed query cannot return Complete stage,
    /// since the parent query need to aggregate the results after.
    if (to_stage == QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::WithMergeableState;

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    if (getClusterQueriedNodes(settings, cluster) == 1)
        return QueryProcessingStage::Complete;

    if (settings.optimize_skip_unused_shards && settings.optimize_distributed_group_by_sharding_key && sharding_key_expr
        && (settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic))
    {
        Block sharding_key_block = sharding_key_expr->getSampleBlock();
        auto stage = getOptimizedQueryProcessingStage(query_info.query, settings.extremes, sharding_key_block);
        if (stage)
        {
            LOG_DEBUG(log, "Force processing stage to {}", QueryProcessingStage::toString(*stage));
            return *stage;
        }
    }

    return QueryProcessingStage::WithMergeableState;
}

/// New functions

IColumn::Selector StorageDistributedMergeTree::createSelector(const ColumnWithTypeAndName & result) const
{
/// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
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

#if 0
    auto selector = createSelector(key_column);

    for (size_t i = 0; i < key_column.column->size(); ++i)
    {
        std::cout << "key=" << key_column.column->getInt(i) << ", selector=" << selector[i] << "\n";
    }

    return selector;
#endif
}

const ExpressionActionsPtr & StorageDistributedMergeTree::getShardingKeyExpr() const
{
    return sharding_key_expr;
}

size_t StorageDistributedMergeTree::getRandomShardIndex()
{
    std::lock_guard lock(rng_mutex);
    return std::uniform_int_distribution<size_t>(0, shards - 1)(rng);
}

IDistributedWriteAheadLog::RecordSequenceNumber StorageDistributedMergeTree::lastSequenceNumber() const
{
    std::lock_guard lock(sns_mutex);
    return last_sn;
}

StorageDistributedMergeTree::WriteCallbackData * StorageDistributedMergeTree::writeCallbackData(const String & query_status_poll_id, UInt16 block_id)
{
    assert(!query_status_poll_id.empty());

    auto added = ingesting_blocks.add(query_status_poll_id, block_id);
    assert(added);
    (void)added;

    return new WriteCallbackData{query_status_poll_id, block_id, this};
}

void StorageDistributedMergeTree::writeCallback(
    const IDistributedWriteAheadLog::AppendResult & result, const String & query_status_poll_id, UInt16 block_id)
{
    if (result.err)
    {
        ingesting_blocks.fail(query_status_poll_id, result.err);
        LOG_ERROR(log, "Failed to write block={} for query_status_poll_id={} error={}", block_id, query_status_poll_id, result.err);
    }
    else
    {
        ingesting_blocks.remove(query_status_poll_id, block_id);
    }
}

void StorageDistributedMergeTree::writeCallback(const IDistributedWriteAheadLog::AppendResult & result, void * data)
{
    auto pdata = static_cast<WriteCallbackData *>(data);
    pdata->storage->writeCallback(result, pdata->query_status_poll_id, pdata->block_id);
    delete pdata;
}

/// Merge `rhs` block to `lhs`
/// FIXME : revisit SquashingTransform::append
void StorageDistributedMergeTree::mergeBlocks(Block & lhs, Block & rhs)
{
    auto lhs_rows = lhs.rows();

    for (auto & rhs_col : rhs)
    {
        ColumnWithTypeAndName * lhs_col = lhs.findByName(rhs_col.name);
        /// FIXME: check datatype, schema changes

        if (unlikely(lhs_col == nullptr))
        {
            /// lhs doesn't have this column
            ColumnWithTypeAndName new_col{rhs_col.cloneEmpty()};

            /// what about column with default expression
            new_col.column->assumeMutable()->insertManyDefaults(lhs_rows);
            lhs.insert(std::move(new_col));
            lhs_col = lhs.findByName(rhs_col.name);
        }
        lhs_col->column->assumeMutable()->insertRangeFrom(*rhs_col.column.get(), 0, rhs_col.column->size());
    }

    lhs.checkNumberOfRows();
}

void StorageDistributedMergeTree::commitSNLocal(IDistributedWriteAheadLog::RecordSequenceNumber commit_sn)
{
    try
    {
        storage->commitSN(commit_sn);
        last_commit_ts = std::chrono::steady_clock::now();

        LOG_INFO(
            log, "Committed offset={} for shard={} to local file system", commit_sn, shard);

        std::lock_guard lock(sns_mutex);
        local_sn = commit_sn;
    }
    catch (...)
    {
        /// It is ok as next commit will override this commit if it makes through
        LOG_ERROR(
            log,
            "Failed to commit offset={} for shard={} to local file system exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StorageDistributedMergeTree::commitSNRemote(IDistributedWriteAheadLog::RecordSequenceNumber commit_sn, std::any & dwal_consume_ctx)
{
    /// Commit sequence number to dwal
    try
    {
        auto err = dwal->commit(commit_sn, dwal_consume_ctx);
        if (unlikely(err != 0))
        {
            /// It is ok as next commit will override this commit if it makes through
            LOG_ERROR(log, "Failed to commit sequence={} for shard={} to dwal error={}", commit_sn, shard, err);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit sequence={} for shard={} to dwal exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StorageDistributedMergeTree::commitSN(std::any & dwal_consume_ctx)
{
    size_t outstanding_sns_size = 0;
    size_t local_committed_sns_size = 0;

    IDistributedWriteAheadLog::RecordSequenceNumber commit_sn = -1;
    Int64 outstanding_commits = 0;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != prev_sn)
        {
            outstanding_commits = last_sn - local_sn;
            commit_sn = last_sn;
            prev_sn = last_sn;
        }
        outstanding_sns_size = outstanding_sns.size();
        local_committed_sns_size = local_committed_sns.size();
    }

    LOG_DEBUG(
        log,
        "Sequence outstanding_sns_size={} local_committed_sns_size={} for shard={}",
        outstanding_sns_size,
        local_committed_sns_size,
        shard);

    if (commit_sn < 0)
    {
        return;
    }

    /// Commit sequence number to local file system every 100 records
    if (outstanding_commits >= 100)
    {
        commitSNLocal(commit_sn);
    }

    commitSNRemote(commit_sn, dwal_consume_ctx);
}

inline void StorageDistributedMergeTree::progressSequencesWithLock(const SequencePair & seq)
{
    assert(!outstanding_sns.empty());

    if (seq != outstanding_sns.front())
    {
        /// Out of order committed sn
        local_committed_sns.insert(seq);
        return;
    }

    last_sn = seq.second;

    outstanding_sns.pop_front();

    /// Find out the max offset we can commit
    while (!local_committed_sns.empty())
    {
        auto & p = outstanding_sns.front();
        if (*local_committed_sns.begin() == p)
        {
            /// sn shall be consecutive
            assert(p.first == last_sn + 1);

            last_sn = p.second;

            local_committed_sns.erase(local_committed_sns.begin());
            outstanding_sns.pop_front();
        }
        else
        {
            break;
        }
    }

    assert(outstanding_sns.size() >= local_committed_sns.size());
    assert(last_sn >= prev_sn);
}

inline void StorageDistributedMergeTree::progressSequences(const SequencePair & seq)
{
    std::lock_guard lock(sns_mutex);
    progressSequencesWithLock(seq);
}

void StorageDistributedMergeTree::doCommit(
    Block block, SequencePair seq_pair, std::shared_ptr<std::vector<String>> keys, std::any & dwal_consume_ctx)
{
    {
        std::lock_guard lock(sns_mutex);
        /// We are sequentially consuming records, so seq_pair is always increasing
        outstanding_sns.push_back(seq_pair);

        /// After deduplication, we may end up with empty block
        /// We still mark these deduped blocks committed and moving forward
        /// the offset checkpointing
        if (!block)
        {
            progressSequencesWithLock(seq_pair);
            return;
        }

        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// Commit blocks to file system async
    part_commit_pool.scheduleOrThrowOnError([&,
                                             moved_block = std::move(block),
                                             moved_seq = std::move(seq_pair),
                                             moved_keys = std::move(keys),
                                             this] {
        LOG_DEBUG(log, "Committing rows={} for shard={} to file system", moved_block.rows(), shard);

        while (1)
        {
            try
            {
                auto output_stream = storage->write(nullptr, storage->getInMemoryMetadataPtr(), global_context);

                /// Setup sequence numbers to persistent them to file system
                static_cast<MergeTreeBlockOutputStream *>(output_stream.get())
                    ->setSequenceInfo(std::make_shared<SequenceInfo>(moved_seq.first, moved_seq.second, moved_keys));

                output_stream->writePrefix();
                output_stream->write(moved_block);
                output_stream->writeSuffix();
                output_stream->flush();
                break;
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "Failed to commit rows={} for shard={} exception={} to file system",
                    moved_block.rows(),
                    shard,
                    getCurrentExceptionMessage(true, true));
                /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }

        progressSequences(moved_seq);
    });

    assert(!block);
    assert(!keys);

    commitSN(dwal_consume_ctx);
}

/// Add with lock held
inline void StorageDistributedMergeTree::addIdempotentKey(const String & key)
{
    if (idem_keys.size() >= global_context.getSettingsRef().max_idempotent_ids)
    {
        auto removed = idem_keys_index.erase(*idem_keys.front());
        (void)removed;
        assert(removed == 1);

        idem_keys.pop_front();
    }

    auto shared_key = std::make_shared<String>(key);
    idem_keys.push_back(shared_key);

    auto [iter, inserted] = idem_keys_index.emplace(*shared_key);
    assert(inserted);
    (void)inserted;
    (void)iter;

    assert(idem_keys.size() == idem_keys_index.size());
}

bool StorageDistributedMergeTree::dedupBlock(const IDistributedWriteAheadLog::RecordPtr & record)
{
    if (!record->hasIdempotentKey())
    {
        return false;
    }

    auto & idem_key = record->idempotentKey();
    auto key_exists = false;
    {
        std::lock_guard lock{sns_mutex};
        key_exists = idem_keys_index.contains(idem_key);
        if (!key_exists)
        {
            addIdempotentKey(idem_key);
        }
    }

    if (key_exists)
    {
        LOG_INFO(log, "Skipping duplicate block, idempotent_key={} offset={}", idem_key, record->sn);
        return true;
    }
    return false;
}

void StorageDistributedMergeTree::commit(const IDistributedWriteAheadLog::RecordPtrs & records, std::any & dwal_consume_ctx)
{
    if (records.empty())
    {
        return;
    }

    Block block;
    auto keys = std::make_shared<std::vector<String>>();

    for (auto & rec : records)
    {
        if (likely(rec->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK))
        {
            if (dedupBlock(rec))
            {
                continue;
            }

            if (likely(block))
            {
                /// Merge next block
                mergeBlocks(block, rec->block);
            }
            else
            {
                /// First block
                block.swap(rec->block);
                assert(!rec->block);
            }

            if (rec->hasIdempotentKey())
            {
                keys->push_back(std::move(rec->idempotentKey()));
            }
        }
        else if (rec->op_code == IDistributedWriteAheadLog::OpCode::ALTER_DATA_BLOCK)
        {
            /// FIXME: execute the later before doing any ingestion
            throw Exception("Not impelemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    doCommit(std::move(block), std::make_pair(records.front()->sn, records.back()->sn), std::move(keys), dwal_consume_ctx);
    assert(!block);
    assert(!keys);
}

IDistributedWriteAheadLog::RecordSequenceNumber StorageDistributedMergeTree::sequenceNumberLoaded() const
{
    std::lock_guard lock(sns_mutex);
    if (local_sn >= 0)
    {
        /// Sequence number committed on disk is offset of a record
        /// `plus one` is the next offset expecting
        return local_sn + 1;
    }

    /// STORED
    return -1000;
}

void StorageDistributedMergeTree::backgroundConsumer()
{
    /// Sleep a while to let librdkafka to populate topic / partition metadata
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    auto topic = getStorageID().getFullTableName();
    setThreadName("DistMergeTree");

    DistributedWriteAheadLogKafkaContext consume_ctx{topic, shard, sequenceNumberLoaded()};
    consume_ctx.auto_offset_reset = dwal_auto_offset_reset;

    auto ssettings = storage_settings.get();
    consume_ctx.consume_callback_timeout_ms = ssettings->distributed_flush_threshhold_ms.value;
    consume_ctx.consume_callback_max_rows = ssettings->distributed_flush_threshhold_count;
    consume_ctx.consume_callback_max_messages_size = ssettings->distributed_flush_threshhold_size;

    LOG_INFO(
        log,
        "Start consuming records from shard={} sequence={} distributed_flush_threshhold_ms={} "
        "distributed_flush_threshhold_count={} "
        "distributed_flush_threshhold_size={}",
        shard,
        consume_ctx.offset,
        consume_ctx.consume_callback_timeout_ms,
        consume_ctx.consume_callback_max_rows,
        consume_ctx.consume_callback_max_messages_size);

    std::any dwal_consume_ctx{consume_ctx};

    struct CallbackData
    {
        StorageDistributedMergeTree * storage;
        std::any & ctx;

        CallbackData(StorageDistributedMergeTree * storage_, std::any & ctx_) : storage(storage_), ctx(ctx_) { }
    };

    CallbackData callback_data{this, dwal_consume_ctx};

    /// The callback is happening in the same thread as the caller
    auto callback = [](IDistributedWriteAheadLog::RecordPtrs records, void * data) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        auto cdata = static_cast<CallbackData *>(data);

        try
        {
            cdata->storage->commit(records, cdata->ctx);
        }
        catch (...)
        {
            LOG_ERROR(
                cdata->storage->log,
                "Failed to commit data for shard={}, exception={}",
                cdata->storage->shard,
                getCurrentExceptionMessage(true, true));
        }
    };

    while (!stopped.test())
    {
        try
        {
            auto err = dwal->consume(callback, &callback_data, dwal_consume_ctx);
            if (err != ErrorCodes::OK)
            {
                LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, err);
                /// FIXME, more error code handling
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }

            /// Check if we have something to commit
            /// Every 5 seconds, flush the local file system checkpoint
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_commit_ts).count() >= 5)
            {
                IDistributedWriteAheadLog::RecordSequenceNumber remote_commit_sn = -1;
                IDistributedWriteAheadLog::RecordSequenceNumber commit_sn = -1;
                {
                    std::lock_guard lock(sns_mutex);
                    if (last_sn != local_sn)
                    {
                        commit_sn = last_sn;
                    }

                    if (prev_sn != last_sn)
                    {
                        remote_commit_sn = last_sn;
                        prev_sn = last_sn;
                    }
                }

                if (commit_sn >= 0)
                {
                    commitSNLocal(commit_sn);
                }

                if (remote_commit_sn >= 0)
                {
                    commitSNRemote(remote_commit_sn, dwal_consume_ctx);
                }
                last_commit_ts = now;
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to consume data for shard={}, exception={}", shard, getCurrentExceptionMessage(true, true));

            throw;
        }
    }

    dwal->stopConsume(dwal_consume_ctx);

    /// When tearing down, commit whatever it has
    commitSN(dwal_consume_ctx);

    IDistributedWriteAheadLog::RecordSequenceNumber commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
        {
            commit_sn = last_sn;
        }
    }

    if (commit_sn >= 0)
    {
        commitSNLocal(commit_sn);
    }
}

void StorageDistributedMergeTree::initWal()
{
    auto ssettings = storage_settings.get();
    auto & offset_reset = ssettings->dwal_auto_offset_reset.value;
    if (offset_reset == "earliest" || offset_reset == "latest")
    {
        dwal_auto_offset_reset = offset_reset;
    }
    else
    {
        throw Exception("Invalid dwal_auto_offset_reset, only 'earliest' and 'latest' are supported", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    auto acks = ssettings->dwal_request_required_acks.value;
    if (acks >= -1 && acks <= replication_factor)
    {
        dwal_request_required_acks = acks;
    }
    else
    {
        throw Exception(
            "Invalid dwal_request_required_acks, shall be in [-1, " + std::to_string(replication_factor) + "] range",
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    auto timeout = ssettings->dwal_request_timeout_ms.value;
    if (timeout > 0)
    {
        dwal_request_timeout_ms = timeout;
    }

    shard = ssettings->shard.value;

    if (ssettings->dwal_cluster_id.value.empty())
    {
        dwal = DistributedWriteAheadLogPool::instance(global_context).getDefault();
    }
    else
    {
        dwal = DistributedWriteAheadLogPool::instance(global_context).get(ssettings->dwal_cluster_id.value);
    }

    if (!dwal)
    {
        throw Exception("Invalid Kafka cluster id " + ssettings->dwal_cluster_id.value, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    /// Cached ctx, reused by append. Multiple threads are accessing append context
    /// since librdkafka topic handle is thread safe, so we are good
    /// FIXME, take care of kafka naming restrictive
    auto topic = getStorageID().getFullTableName();
    DistributedWriteAheadLogKafkaContext append_ctx{topic};
    append_ctx.request_required_acks = dwal_request_required_acks;
    append_ctx.request_timeout_ms = dwal_request_timeout_ms;
    append_ctx.topic_handle = static_cast<DistributedWriteAheadLogKafka *>(dwal.get())->initProducerTopic(append_ctx);
    dwal_append_ctx = append_ctx;
}
}
