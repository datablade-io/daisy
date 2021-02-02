#pragma once

#include "IDistributedWriteAheadLog.h"

struct rd_kafka_s;
struct rd_kafka_conf_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DB
{
/// DistributedWriteAheadLogKafkaContext is not thread safe, so each produce/consume thread
/// shall maintain its own context instance
struct DistributedWriteAheadLogKafkaContext
{
    String topic;
    Int64 partition;
    UInt64 partition_key;
    std::shared_ptr<rd_kafka_topic_s> topic_handle;

    DistributedWriteAheadLogKafkaContext(const String & topic_, Int64 partition_, UInt64 partition_key_);
};

struct KafkaStats {
    UInt64 received = 0;
    UInt64 dropped = 0;
    UInt64 failed = 0;
    UInt64 bytes = 0;
    String pstat;
};

struct DistributedWriteAheadLogKafkaSettings
{
    /// comma separated host/port: host1:port,host2:port,...
    String brokers;
    String protocol = "plaintext";
    UInt32 required_acks = 1;
    UInt32 retries = 1;
    /// other settings
};

class DistributedWriteAheadLogKafka final : public IDistributedWriteAheadLog
{
public:
    explicit DistributedWriteAheadLogKafka(std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings_);
    ~DistributedWriteAheadLogKafka() override;

    void startup() override;
    void shutdown() override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    RecordSequenceNumber append(Record & record, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    Records consume(size_t count, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    void commit(const RecordSequenceNumbers & sequence_numbers, std::any & ctx) override;

private:
    // rdkafka callbacks
    void log_failed_msg(struct rd_kafka_s * rk, const struct rd_kafka_message_s * msg);

    void log_stats(struct rd_kafka_s * rk, char * json, size_t json_len);

    void log_err(struct rd_kafka_s * rk, int err, const char* reason);

    void log_throttle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms);

private:
    static void log_failed_msg(struct rd_kafka_s * rk, const struct rd_kafka_message_s * msg, void * opaque);

    static int log_stats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque);

    static void log_err(struct rd_kafka_s * rk, int err, const char * reason, void * opaque);

    static void log_throttle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque);

private:
    using FreeRdKafka = void (*)(struct rd_kafka_s*);
    using FreeRdKafkaTopic = void (*)(struct rd_kafka_topic_s *);

private:
    void init_producer();
    void init_consumer();
    std::shared_ptr<rd_kafka_topic_s> init_topic(const String & topic);

private:
    std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings;

    Poco::Logger * log;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::unique_ptr<struct rd_kafka_s, FreeRdKafka> rd_kafka_handle;

    std::atomic_uint64_t received = 0;
    std::atomic_uint64_t dropped = 0;
    std::atomic_uint64_t failed = 0;
    std::atomic_uint64_t bytes = 0;

    // produce statistics
    String pstat;
};
}
