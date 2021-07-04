#pragma once

#include "KafkaWAL.h"

struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DB
{
namespace DWAL
{

struct TopicPartionOffset
{
    String topic;
    Int32 partition = -1;
    Int64 offset = -1;

    TopicPartionOffset(const String & topic_, Int32 partition_, Int64 offset_) : topic(topic_), partition(partition_), offset(offset_) { }

    TopicPartionOffset() { }
};

using TopicPartionOffsets = std::vector<TopicPartionOffset>;


class MordenKafkaWAL final
{
public:
    explicit MordenKafkaWAL(std::unique_ptr<KafkaWALSettings> settings_);
    ~MordenKafkaWAL();

    void startup();
    void shutdown();

    Int32 addConsumptions(const TopicPartionOffsets & partitions);
    Int32 removeConsumptions(const TopicPartionOffsets & partitions);

    WAL::ConsumeResult consume(Int32 timeout_ms);

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
    using StatsPtr = std::shared_ptr<Stats>;

private:
    void initConsumer();

private:
    std::unique_ptr<KafkaWALSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::mutex partitions_mutex;
    std::unordered_map<String, std::vector<Int32>> partitions;

    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

    RdKafkaHandlePtr consumer_handle;

    Poco::Logger * log;

    StatsPtr stats;
};

using MordenKafkaWALPtr = std::shared_ptr<MordenKafkaWAL>;
using MordenKafkaWALPtrs = std::vector<MordenKafkaWALPtr>;
}
}
