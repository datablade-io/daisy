#include "IDistributedWriteAheadLog.h"

#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
std::vector<UInt8> IDistributedWriteAheadLog::Record::serialize(const Record & record)
{
    std::vector<UInt8> data;
    data.reserve(static_cast<size_t>((record.block.bytes() + 1) * 1.1));

    WriteBufferFromVector wb{data};
    auto output = std::make_shared<MaterializingBlockOutputStream>(std::make_shared<NativeBlockOutputStream>(wb, 0, Block{}), Block{});
    writeIntBinary(IDistributedWriteAheadLog::WAL_VERSION, wb);
    writeIntBinary(record.action_type, wb);
    output->write(record.block);
    output->flush();
    wb.finalize();
    return data;
}

IDistributedWriteAheadLog::Record IDistributedWriteAheadLog::Record::deserialize(const std::vector<UInt8> & data)
{
    /// FIXME
    (void)data;
    return Record{IDistributedWriteAheadLog::ActionType::ADD_DATA_BLOCK, Block{}};
}
}
