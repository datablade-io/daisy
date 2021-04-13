#pragma once

#include "IDistributedWriteAheadLog.h"

#include <Common/ThreadPool.h>


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
    Int32 partition = -1;

    /// - absolute offset (0..N)
    /// - RD_KAFKA_OFFSET_BEGINNING, -2
    /// - RD_KAFKA_OFFSET_END, -1
    /// - RD_KAFKA_OFFSET_STORED, -1000
    /// - RD_KAFKA_OFFSET_TAIL
    Int64 offset = -1000;

    /// ************************************************

    /// admin API settings
    Int32 partitions = 1;
    Int32 replication_factor = 1;

    /// ************************************************

    /// per topic producer settings
    Int32 request_required_acks = 1;
    Int32 request_timeout_ms = 30000;

    /// ************************************************

    /// per topic consumer settings
    String auto_offset_reset = "earliest";

    /// per topic librdkafka client side settings for consumer
    /// Int32 consume_callback_max_messages = 1000000;

    static String topicPartitonKey(const String & topic, Int32 partition) { return topic + "$" + std::to_string(partition); }

    String key() { return topicPartitonKey(topic, partition); }

    /// cached topic handle across call
    std::shared_ptr<rd_kafka_topic_s> topic_handle;

    DistributedWriteAheadLogKafkaContext(const String & topic_, Int32 partition_, Int64 offset_)
        : topic(topic_), partition(partition_), offset(offset_)
    {
    }

    explicit DistributedWriteAheadLogKafkaContext(const String & topic_) : topic(topic_) { }
};

struct KafkaStats
{
    UInt64 received = 0;
    UInt64 dropped = 0;
    UInt64 failed = 0;
    UInt64 bytes = 0;
    String pstat;
};

struct DistributedWriteAheadLogKafkaSettings
{
    String cluster_id;

    /// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    /// global settings for both producer and consumer /// global metrics
    /// comma separated host/port: host1:port,host2:port,...
    String brokers;
    String security_protocol = "plaintext";
    /// FIXME, SASL, SSL etc support

    Int32 message_max_bytes = 1000000;
    Int32 topic_metadata_refresh_interval_ms = 300000;
    Int32 statistic_internal_ms = 30000;
    String debug = "";

    /////////////////////////////////////////////////////

    /// global settings for producer
    /// String transactional_id;
    /// Int32 transaction_timeout_ms = 60000;
    bool enable_idempotence = true;
    Int32 queue_buffering_max_messages = 100000;
    Int32 queue_buffering_max_kbytes = 1048576;
    Int32 queue_buffering_max_ms = 5; /// same as linger.ms
    Int32 message_send_max_retries = 2;
    Int32 retry_backoff_ms = 100;
    /// none, gzip, snappy, lz4, zstd, inherit
    String compression_codec = "snappy";

    /// global librdkafka client side settings for producer
    Int32 message_timeout_ms = 40000;
    Int32 message_delivery_async_poll_ms = 100;
    Int32 message_delivery_sync_poll_ms = 10;

    /////////////////////////////////////////////////////

    /// global settings for consumer
    String group_id = "";
    /// String group_instance_id
    /// String partition_assignment_strategy
    /// Int32 session_timeout_ms = 10000;
    /// Int32 max_poll_interval_ms = 30000;
    bool enable_auto_commit = true;
    bool check_crcs = false;
    Int32 auto_commit_interval_ms = 5000;
    Int32 fetch_message_max_bytes = 1048576;

    /// global librdkafka client side settings for consumer
    Int32 queued_min_messages = 1000000;
    Int32 queued_max_messages_kbytes = 65536;
};

class DistributedWriteAheadLogKafka final : public IDistributedWriteAheadLog
{
public:
    explicit DistributedWriteAheadLogKafka(std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings_);
    ~DistributedWriteAheadLogKafka() override;

    void startup() override;
    void shutdown() override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    AppendResult append(const Record & record, std::any & ctx) override;

    /// Async append, we don't poll result but rely on callback to deliver the result back
    /// `ctx` is DistributedWriteAheadLogKafkaContext
    Int32 append(const Record & record, AppendCallback callback, void * data, std::any & ctx) override;

    void poll(Int32 timeout_ms, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    /// register a consumer callback for topic, partition
    Int32 consume(ConsumeCallback callback, void * data, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    ConsumeResult consume(UInt32 count, Int32 timeout_ms, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    /// Stop consuming
    Int32 stopConsume(std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    /// `commit` doesn't really commit offsets to Kafka brokers instead it stores offsets in
    /// memory and will be later committed in batch (every `auto_commit_internval_ms`). It doesn't
    /// commit offset synchronously because of performance concerns
    Int32 commit(RecordSequenceNumber sequence_number, std::any & ctx) override;

    /// APIs for clients to cache the topic handle
    std::shared_ptr<rd_kafka_topic_s> initProducerTopic(const DistributedWriteAheadLogKafkaContext & ctx);
    std::shared_ptr<rd_kafka_topic_s> initConsumerTopic(const DistributedWriteAheadLogKafkaContext & ctx);

    /// Admin APIs
    /// `ctx` is DistributedWriteAheadLogKafkaContext
    Int32 create(const String & name, std::any & ctx) override;

    /// `ctx` is DistributedWriteAheadLogKafkaContext
    Int32 remove(const String & name, std::any & ctx) override;


private:
    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

    struct DeliveryReport
    {
        std::atomic_int32_t partition = -1;
        std::atomic_int64_t offset = -1;
        std::atomic_int32_t err = 0;
        AppendCallback callback = nullptr;
        void * data = nullptr;
        bool delete_self = false;
        explicit DeliveryReport(AppendCallback callback_ = nullptr, void * data_ = nullptr, bool delete_self_ = false)
            : callback(callback_), data(data_), delete_self(delete_self_)
        {
        }
    };

private:
    void initProducer();
    void initConsumer();

    /// poll delivery report
    void backgroundPollProducer();

    /// poll errors
    void backgroundPollConsumer();

    Int32 initConsumerTopicHandleIfNecessary(DistributedWriteAheadLogKafkaContext & walctx);

    AppendResult handleError(int err, const Record & record, const DistributedWriteAheadLogKafkaContext & ctx);

#if 0
    void flush(std::unordered_map<String, IDistributedWriteAheadLog::RecordPtrs> & buffer);
#endif

    /// DeliveryReport `dr` must reside on heap. if `do_append` succeeds, the DeliveryReport object is handled over
    /// to librdkafka and will be used by `delivery_report` callback eventually
    Int32 doAppend(const Record & record, DeliveryReport * dr, DistributedWriteAheadLogKafkaContext & walctx);

private:
    static void deliveryReport(struct rd_kafka_s *, const rd_kafka_message_s * rkmessage, void * /*opaque*/);

public:
    struct Stats
    {
        std::atomic_uint64_t received = 0;
        std::atomic_uint64_t dropped = 0;
        std::atomic_uint64_t failed = 0;
        std::atomic_uint64_t bytes = 0;

        // produce statistics
        String pstat;

        Poco::Logger * log;

        explicit Stats(Poco::Logger * log_) : log(log_) { }
    };
    using StatsPtr = std::unique_ptr<Stats>;

private:
    std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    RdKafkaHandlePtr producer_handle;
    RdKafkaHandlePtr consumer_handle;

#if 0
    std::mutex consumer_callbacks_mutex;
    std::unordered_map<String, std::pair<IDistributedWriteAheadLog::ConsumeCallback, void *>> consumer_callbacks;
#endif

    ThreadPool poller;

    StatsPtr stats;

    Poco::Logger * log;
};
}
