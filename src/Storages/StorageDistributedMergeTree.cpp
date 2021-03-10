#include "StorageDistributedMergeTree.h"

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Pipe.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Storages/MergeTree/DistributedMergeTreeBlockOutputStream.h>
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
}

namespace
{
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
        /// virtual table which is for data ingestion only
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
    storage->read(query_plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
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
    return storage->read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
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

    auto topic = getStorageID().getFullTableName();
    LOG_INFO(log, topic + " stopping");
    if (storage)
    {
        tailer->wait();
        storage->shutdown();
    }
    LOG_INFO(log, topic + " stopped");
}

String StorageDistributedMergeTree::getName() const
{
    return "DistributedMergeTree";
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

///////////////////////// NEW FUNCTIONS //////////////////////

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

IDistributedWriteAheadLog::RecordSequenceNumber StorageDistributedMergeTree::lastSequenceNumber()
{
    /// FIXME, load from ckpt file
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

void StorageDistributedMergeTree::commitSN(std::any & dwal_consume_ctx)
{
    const auto & dwalctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(dwal_consume_ctx);

    size_t outstanding_sns_size = 0;
    size_t local_committed_sns_size = 0;

    IDistributedWriteAheadLog::RecordSequenceNumber commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != prev_sn)
        {
            commit_sn = last_sn;
            prev_sn = last_sn;
        }
        outstanding_sns_size = outstanding_sns.size();
        local_committed_sns_size = local_committed_sns.size();
    }

    LOG_TRACE(
        log,
        "Sequence outstanding_sns_size={} local_committed_sns_size={} for topic={} partition={}",
        outstanding_sns_size,
        local_committed_sns_size,
        dwalctx.topic,
        dwalctx.partition);

    if (commit_sn < 0)
    {
        return;
    }

    try
    {
        /// Commit sequence number to dwal
        auto err = dwal->commit(commit_sn, dwal_consume_ctx);
        if (unlikely(err != 0))
        {
            /// it is ok as next commit will override this commit if it makes through
            LOG_ERROR(log, "Failed to commit offset={} for topic={} partition={} error={}", commit_sn, dwalctx.topic, dwalctx.partition, err);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit offset={} for topic={} partition={} exception={}",
            commit_sn,
            dwalctx.topic,
            dwalctx.partition,
            getCurrentExceptionMessage(true, true));
    }
}

void StorageDistributedMergeTree::doCommit(Block && block, const SequencePair & seq_pair, std::any & dwal_consume_ctx)
{
    assert(block);

    {
        std::lock_guard lock(sns_mutex);
        /// We are sequentially consuming records, so seq_pair is always increasing
        outstanding_sns.push_back(seq_pair);

        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// Commit blocks to file system async
    part_commit_pool.scheduleOrThrowOnError([&, seq = seq_pair, moved_block = std::move(block), this] {
        const auto & dwalctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(dwal_consume_ctx);

        LOG_DEBUG(log, "Committing rows={} for topic={} partition={} to file system", moved_block.rows(), dwalctx.topic, dwalctx.partition);

        while (1)
        {
            try
            {
                /// FIXME : write offset to file system
                auto output_stream = storage->write(nullptr, storage->getInMemoryMetadataPtr(), global_context);
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
                    "Failed to commit rows={} for topic={} partition={} exception={} to file system",
                    moved_block.rows(),
                    dwalctx.topic,
                    dwalctx.partition,
                    getCurrentExceptionMessage(true, true));
                /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }

        std::lock_guard lock(sns_mutex);

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
    });

    assert(!block);

    commitSN(dwal_consume_ctx);
}

void StorageDistributedMergeTree::commit(const IDistributedWriteAheadLog::RecordPtrs & records, std::any & dwal_consume_ctx)
{
    if (records.empty())
    {
        return;
    }

    Block block;

    for (auto & rec : records)
    {
        if (likely(rec->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK))
        {
            if (likely(block))
            {
                /// Merge next block
                mergeBlocks(block, rec->block);
            }
            else
            {
                /// first block
                block.swap(rec->block);
                assert(!rec->block);
            }
        }
        else if (rec->op_code == IDistributedWriteAheadLog::OpCode::ALTER_DATA_BLOCK)
        {
            /// FIXME: execute the later before doing any ingestion
            throw Exception("Not impelemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    doCommit(std::move(block), std::make_pair(records.front()->sn, records.back()->sn), dwal_consume_ctx);
    assert(!block);
}

void StorageDistributedMergeTree::backgroundConsumer()
{
    /// std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    auto topic = getStorageID().getFullTableName();
    setThreadName("DistMergeTree");

    DistributedWriteAheadLogKafkaContext consume_ctx{topic, shard, lastSequenceNumber()};
    consume_ctx.auto_offset_reset = dwal_auto_offset_reset;

    auto ssettings = storage_settings.get();
    consume_ctx.consume_callback_timeout_ms = ssettings->distributed_flush_threshhold_ms.value;
    consume_ctx.consume_callback_max_rows = ssettings->distributed_flush_threshhold_count;
    consume_ctx.consume_callback_max_messages_size = ssettings->distributed_flush_threshhold_size;

    LOG_INFO(
        log,
        "Start consuming records from topic={} partition={} distributed_flush_threshhold_ms={} distributed_flush_threshhold_count={} "
        "distributed_flush_threshhold_size={}",
        topic,
        shard,
        consume_ctx.consume_callback_timeout_ms,
        consume_ctx.consume_callback_max_rows,
        consume_ctx.consume_callback_max_messages_size);

    std::any dwal_consume_ctx{consume_ctx};

    using CallbackData = std::tuple<StorageDistributedMergeTree *, String &, std::any &>;
    CallbackData callback_data{this, topic, dwal_consume_ctx};

    /// The callback is happening in the same thread as the caller
    auto callback = [](IDistributedWriteAheadLog::RecordPtrs records, void * data) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        auto cdata = static_cast<CallbackData *>(data);

        try
        {
            std::get<0>(*cdata)->commit(records, std::get<2>(*cdata));
        }
        catch (...)
        {
            LOG_ERROR(
                std::get<0>(*cdata)->log,
                "Failed to commit data for topic={} partition={}, exception={}",
                std::get<1>(*cdata),
                std::get<0>(*cdata)->shard,
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
                LOG_ERROR(log, "Failed to consume data for topic={} partition={}, error={}", topic, shard, err);
                /// FIXME, more error code handling
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "Failed to consume data for topic={} partition={}, exception={}",
                topic,
                shard,
                getCurrentExceptionMessage(true, true));

            throw;
        }
    }

    dwal->stopConsume(dwal_consume_ctx);
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

    /// cached ctx, reused by append. Multiple threads are accessing append context
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
