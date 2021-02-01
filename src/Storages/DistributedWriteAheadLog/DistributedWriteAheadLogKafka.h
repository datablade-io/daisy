#pragma once

#include "IDistributedWriteAheadLog.h"

namespace DB
{
struct DistributedWriteAheadLogKafkaContext
{
    String topic;
    Int64 partition;
    UInt64 partition_key;

    DistributedWriteAheadLogKafkaContext(const String & topic_, Int64 partition_, UInt64 partition_key_)
        : topic(topic_), partition(partition_), partition_key(partition_key_)
    {
    }
};

struct DistributedWriteAheadLogKafkaSettings
{
    String brokers;
    /// other settings
};

class DistributedWriteAheadLogKafka : public IDistributedWriteAheadLog
{
public:
    explicit DistributedWriteAheadLogKafka(const DistributedWriteAheadLogKafkaSettings & settings_);

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    RecordSequenceNumber append(Record & record, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    Records consume(size_t count, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    void commit(const RecordSequenceNumbers & sequence_numbers, std::any & ctx) override;

private:
    DistributedWriteAheadLogKafkaSettings settings;
};
}
