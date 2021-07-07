#pragma once

#include "KafkaWALConsumer.h"
#include "KafkaWALSettings.h"
#include "Results.h"

#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

namespace DWAL
{

/// KafkaWALConsumerMultiplexer has a dedicated thread consuming a list of topic partitions
/// in a dedicated consumer group by using high level KafkaWALConsumer. It then routes
/// the records to different targets by calling the corresponding callbacks.
/// It is multithread safe.
class KafkaWALConsumerMultiplexer final : private boost::noncopyable
{
public:
    /// Please pay attention to the `settings.group_id`. If end user likes to consume
    /// a list of partitions with multiple consumer multiplexers, they will need use
    /// the same `settings.group_id`
    explicit KafkaWALConsumerMultiplexer(std::unique_ptr<KafkaWALSettings> settings);
    ~KafkaWALConsumerMultiplexer();

    void startup();
    void shutdown();

    /// Register a callback for a partition of a topic at specific offset.
    /// Once registered, the callback will be invoked asynchronously in a background thread when there
    /// is new data available, so make sure the callback handles thread safety correctly.
    /// Return true if the subscription is good, otherwise false
    int32_t addSubscription(const TopicPartitionOffset & tpo, ConsumeCallback callback, void * data);

    /// Return true if the subscription is good, otherwise false
    /// Remove the registered callback for a partition of a topic.
    /// `offset` in `tpo` is not inspected
    int32_t removeSubscription(const TopicPartitionOffset & tpo);

    int32_t commit(const TopicPartitionOffset & tpo);

private:
    void backgroundPoll();
    void handleResult(ConsumeResult result) const;

private:
    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::unique_ptr<KafkaWALConsumer> consumer;

    ThreadPool poller;

    mutable std::mutex callbacks_mutex;
    /// callbacks are indexed by `topic` name. For now, we assume in one single node
    /// there is only one unique table / topic
    using CallbackDataPair = std::pair<ConsumeCallback, void *>;
    std::unordered_map<std::string, std::shared_ptr<CallbackDataPair>> callbacks;

    Poco::Logger * log;
};

using KafkaWALConsumerMultiplexerPtr = std::shared_ptr<KafkaWALConsumerMultiplexer>;
using KafkaWALConsumerMultiplexerPtrs = std::vector<KafkaWALConsumerMultiplexerPtr >;

}
