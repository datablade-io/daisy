#pragma once

#include "KafkaWALSettings.h"
#include "KafkaWALStats.h"
#include "Results.h"

#include <Common/ThreadPool.h>

#include <memory>
#include <atomic>

struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DWAL
{
struct KafkaWALContext;

/// KafkaWALSimpleConsumer consumes data from a specific single partition of a topic
class KafkaWALSimpleConsumer final
{
public:
    explicit KafkaWALSimpleConsumer(std::unique_ptr<KafkaWALSettings> settings_);
    ~KafkaWALSimpleConsumer();

    void startup();
    void shutdown();

    /// Register a consumer callback for a partition of a topic
    int32_t consume(ConsumeCallback callback, void * data, KafkaWALContext & ctx);

    ConsumeResult consume(uint32_t count, int32_t timeout_ms, KafkaWALContext & ctx);

    /// Stop consuming for topic, partition
    int32_t stopConsume(KafkaWALContext & ctx);

    int32_t commit(int64_t offset, KafkaWALContext & ctx);

private:
    /// poll errors
    void backgroundPoll();

    void initHandle();

    std::shared_ptr<rd_kafka_topic_s> initTopicHandle(const KafkaWALContext & ctx);

    int32_t initTopicHandleIfNecessary(KafkaWALContext & walctx);

private:
    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

private:
    std::unique_ptr<KafkaWALSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    RdKafkaHandlePtr consumer_handle;

    ThreadPool poller;

    Poco::Logger * log;

    KafkaWALStatsPtr stats;
};
}
