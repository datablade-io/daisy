#pragma once

#include <DistributedWriteAheadLog/Record.h>
#include <DistributedWriteAheadLog/WAL.h>
#include <Interpreters/Context_fwd.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

class StorageDistributedMergeTree;

/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReader final
{
public:
    StreamingBlockReader(const StorageDistributedMergeTree & storage, ContextPtr context_, Int32 shard_, Poco::Logger * log_);
    ~StreamingBlockReader();
    DWAL::RecordPtrs read(Int32 timeout_ms);

private:
    ContextPtr context;
    Int32 shard;
    Poco::Logger * log;

    String streaming_id;
    DWAL::WALPtr dwal;
    std::any consume_ctx;
};
}
