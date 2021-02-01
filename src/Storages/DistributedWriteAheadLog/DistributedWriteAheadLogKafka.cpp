#include "DistributedWriteAheadLogKafka.h"

#include <Common/Macros.h>
#include <Common/config_version.h>

namespace DB
{
DistributedWriteAheadLogKafka::DistributedWriteAheadLogKafka(const DistributedWriteAheadLogKafkaSettings & settings_)
    : settings(settings_)
{
}

IDistributedWriteAheadLog::RecordSequenceNumber DistributedWriteAheadLogKafka::append(Record & record, std::any & ctx)
{
    (void)record;
    (void)ctx;
    return {};
}

IDistributedWriteAheadLog::Records DistributedWriteAheadLogKafka::consume(size_t count, std::any & ctx)
{
    (void)count;
    (void)ctx;
    return {};
}

void DistributedWriteAheadLogKafka::commit(const IDistributedWriteAheadLog::RecordSequenceNumbers & sequence_numbers, std::any & ctx)
{
    (void)sequence_numbers;
    (void)ctx;
}

}
