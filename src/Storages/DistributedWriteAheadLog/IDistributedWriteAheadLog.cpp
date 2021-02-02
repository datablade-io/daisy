#include "IDistributedWriteAheadLog.h"

#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
std::vector<UInt8> IDistributedWriteAheadLog::Record::write(const Record & record)
{
    std::vector<UInt8> data;
    data.reserve(static_cast<size_t>((record.block.bytes() + 2) * 1.5));

    WriteBufferFromVector wb{data};
    auto output = std::make_shared<MaterializingBlockOutputStream>(std::make_shared<NativeBlockOutputStream>(wb, 0, Block{}), Block{});

    /// WAL Version
    writeIntBinary(IDistributedWriteAheadLog::WAL_VERSION, wb);

    /// Action Type
    writeIntBinary(record.action_type, wb);

    /// Data
    output->write(record.block);
    output->flush();

    /// shrink to what has been written
    wb.finalize();
    return data;
}

IDistributedWriteAheadLog::Record IDistributedWriteAheadLog::Record::read(const char * data, size_t size)
{
    ReadBufferFromMemory rb{data, size};

    UInt8 wal_version = 0;
    readIntBinary(wal_version, rb);

    (void)wal_version;

    ActionType action_type = ActionType::UNKNOWN;
    readIntBinary(action_type, rb);

    NativeBlockInputStream input{rb, 0};

    return Record{action_type, input.read()};
}
}
