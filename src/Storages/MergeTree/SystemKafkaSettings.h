#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;

#define SYSTEM_KAFKA_RELATED_SETTINGS(M) \
    M(String, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.", 0) \
    M(String, kafka_topic, "", "Kafka topic.", 0) \
    M(Int64, kafka_partition, -1, "Kafka partition to poll from.", 0) \
    M(UInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.", 0) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, kafka_poll_timeout_ms, 0, "Timeout for single poll from Kafka.", 0) \
    /* default is min(max_block_size, kafka_max_block_size)*/ \
    M(UInt64, kafka_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    /* default is = max_insert_block_size / kafka_num_consumers  */ \
    M(UInt64, kafka_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0)

/** TODO: */
/* https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md */
/* https://github.com/edenhill/librdkafka/blob/v1.4.2/src/rdkafka_conf.c */

#define LIST_OF_SYSTEM_KAFKA_SETTINGS(M) \
    SYSTEM_KAFKA_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(SystemKafkaSettingsTraits, LIST_OF_SYSTEM_KAFKA_SETTINGS)


struct SystemKafkaSettings : public BaseSettings<SystemKafkaSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
