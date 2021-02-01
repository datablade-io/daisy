#pragma once

#include <Core/Block.h>

#include <any>

namespace DB
{
/** Distributed Write Ahead Log (WAL) interfaces which defines an ordered sequence of `transitions`.
 * At its core, it is an sequntial orderded and append-only log abstraction
 * It generally can store any `transition` operation including but not limited by the following ones,
 * as long as the operation can wrap in a `Block` and can be understood in the all partiticipants invovled.
 * 1. Insert a data block (data path)
 * 2. Mutate commands like `ALTER TABLE <table> UPDATE ...`
 * 3. DDL commands like `CREATE TABLE <table> ...`
 * 4. ...
 */

class IDistributedWriteAheadLog : private boost::noncopyable
{
public:
    virtual ~IDistributedWriteAheadLog() = default;

    enum class ActionType : UInt8
    {
        /// Data
        ADD_DATA_BLOCK = 0,
        ALTER_DATA_BLOCK = 1,

        /// Metadata
        CREATE_TABLE = 2,
        DELETE_TABLE = 3,
        ALTER_TABLE = 4,
    };

    using RecordSequenceNumber = Int64;
    using RecordSequenceNumbers = std::vector<Int64>;

    struct Record
    {
        ActionType action_type;
        Block block;
        RecordSequenceNumber sequence_number = -1;

        Record(ActionType action_type_, Block && block_) : action_type(action_type_), block(block_) { }
    };

    using Records = std::vector<Record>;

    constexpr static UInt8 WAL_VERSION = 1;

    /// Append a Record to the target WAL and returns SequenceNumber for this record
    /// Once this function is returned without an error, the record is guaranteed to be committed
    virtual RecordSequenceNumber append(Record & record, std::any & ctx) = 0;

    /// Consume a max number (`count`) of Record
    virtual Records consume(size_t count, std::any & ctx) = 0;

    /// Move the consuming sequence numbers forward
    virtual void commit(const RecordSequenceNumbers & sequence_numbers, std::any & ctx) = 0;
};

using DistributedWriteAheadLogPtr = std::shared_ptr<IDistributedWriteAheadLog>;
}
