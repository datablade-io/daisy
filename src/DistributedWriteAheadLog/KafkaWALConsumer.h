#pragma once

#include "KafkaWALSettings.h"
#include "KafkaWALStats.h"
#include "Results.h"

#include <Common/ThreadPool.h>

struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DWAL
{
struct TopicPartionOffset
{
    std::string topic;
    int32_t partition = -1;
    int64_t offset = -1;

    TopicPartionOffset(const std::string & topic_, int32_t partition_, int64_t offset_)
        : topic(topic_), partition(partition_), offset(offset_)
    {
    }

    TopicPartionOffset() { }
};

using TopicPartionOffsets = std::vector<TopicPartionOffset>;

class KafkaWALConsumer final
{
public:
    explicit KafkaWALConsumer(std::unique_ptr<KafkaWALSettings> settings_);
    ~KafkaWALConsumer();

    void startup();
    void shutdown();

    int32_t addConsumptions(const TopicPartionOffsets & partitions);
    int32_t removeConsumptions(const TopicPartionOffsets & partitions);

    ConsumeResult consume(int32_t timeout_ms);

private:
    void initHandle();
    void backgroundPoll();

private:
    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

private:
    std::unique_ptr<KafkaWALSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::mutex partitions_mutex;
    std::unordered_map<std::string, std::vector<int32_t>> partitions;

    RdKafkaHandlePtr consumer_handle;

    ThreadPool poller;

    Poco::Logger * log;

    KafkaWALStatsPtr stats;
};

using KafkaWALConsumerPtr = std::shared_ptr<KafkaWALConsumer>;
using KafkaWALConsumerPtrs = std::vector<KafkaWALConsumerPtr>;
}
