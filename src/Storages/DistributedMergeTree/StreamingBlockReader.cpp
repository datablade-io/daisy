#include "StreamingBlockReader.h"

#include "StorageDistributedMergeTree.h"

#include <DistributedWriteAheadLog/KafkaWAL.h>
#include <DistributedWriteAheadLog/Name.h>
#include <DistributedWriteAheadLog/WALPool.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OK;
}

StreamingBlockReader::StreamingBlockReader(
    const StorageDistributedMergeTree & storage, ContextPtr context_, Int32 shard_, Poco::Logger * log_)
    : context(context_), shard(shard_), log(log_)
{
    const auto & storage_id = storage.getStorageID();
    auto topic = DWAL::escapeDWalName(storage_id.getDatabaseName(), storage_id.getTableName());
    DWAL::KafkaWALContext kctx{topic, shard, Int64{-1} /* latest */};
    kctx.auto_offset_reset = "latest";

    consume_ctx = std::move(kctx);

    /// FIXME, make sure unique streaming ID
    streaming_id = topic + "-" + context->getCurrentQueryId();
    dwal = DWAL::WALPool::instance(context).getOrCreateStreaming(streaming_id, storage.streamingStorageClusterId());
    assert(dwal);

    LOG_INFO(log, "Start streaming reading from shard={} streaming_id={}", shard, streaming_id);
}

StreamingBlockReader::~StreamingBlockReader()
{
    LOG_INFO(log, "Stop streaming reading from shard={} streaming_id={}", shard, streaming_id);

    dwal->stopConsume(consume_ctx);
    DWAL::WALPool::instance(context).deleteStreaming(streaming_id);
    dwal = nullptr;
}

DWAL::RecordPtrs StreamingBlockReader::read(Int32 timeout_ms)
{
    DWAL::WAL::ConsumeResult result{dwal->consume(100, timeout_ms, consume_ctx)};
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to consume streaming, streaming_id={} err={}", streaming_id, result.err);
        return {};
    }

    return std::move(result.records);
}
}
