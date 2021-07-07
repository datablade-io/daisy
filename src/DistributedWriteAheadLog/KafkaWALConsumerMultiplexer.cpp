#include "KafkaWALConsumerMultiplexer.h"

#include <Common/setThreadName.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}
}

namespace DWAL
{
KafkaWALConsumerMultiplexer::KafkaWALConsumerMultiplexer(std::unique_ptr<KafkaWALSettings> settings)
    : consumer(std::make_unique<KafkaWALConsumer>(std::move(settings))), poller(1), log(&Poco::Logger::get("KafkaWALConsumerMultiplexer"))
{
}

KafkaWALConsumerMultiplexer::~KafkaWALConsumerMultiplexer()
{
    shutdown();
}

void KafkaWALConsumerMultiplexer::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    consumer->startup();

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });

    LOG_INFO(log, "Started");
}

void KafkaWALConsumerMultiplexer::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    consumer->shutdown();
    poller.wait();
    LOG_INFO(log, "Stopped");
}

int32_t KafkaWALConsumerMultiplexer::addSubscription(const TopicPartitionOffset & tpo, ConsumeCallback callback, void * data)
{
    assert(callback && data != nullptr);
    {
        std::lock_guard lock{callbacks_mutex};

        if (callbacks.contains(tpo.topic))
        {
            return false;
        }

        auto res = consumer->addSubscriptions({tpo});
        if (res != DB::ErrorCodes::OK)
        {
            return res;
        }

        callbacks.emplace(tpo.topic, std::make_shared<CallbackDataPair>(callback, data));
    }

    LOG_INFO(log, "Successfully add subscription to topic={} partition={} offset={}", tpo.topic, tpo.partition, tpo.offset);

    return DB::ErrorCodes::OK;
}

int32_t KafkaWALConsumerMultiplexer::removeSubscription(const TopicPartitionOffset & tpo)
{
    {
        std::lock_guard lock{callbacks_mutex};

        auto iter = callbacks.find(tpo.topic);
        if (iter == callbacks.end())
        {
            return false;
        }

        auto res = consumer->removeSubscriptions({tpo});
        if (res != DB::ErrorCodes::OK)
        {
            return res;
        }

        callbacks.erase(iter);
    }

    LOG_INFO(log, "Successfully remove subscription to topic={} partition={}", tpo.topic, tpo.partition);

    return DB::ErrorCodes::OK;
}

void KafkaWALConsumerMultiplexer::backgroundPoll()
{
    LOG_INFO(log, "Polling consumer multiplexer started");
    setThreadName("KWalCMPoller");

    while (!stopped.test())
    {
        auto result = consumer->consume(100000, 1000);

        if (!result.records.empty())
        {
            /// Consume what has been returned regardless the error
            handleResult(std::move(result));
            assert(result.records.empty());
        }
    }

    LOG_INFO(log, "Polling consumer multiplexer stopped");
}

void KafkaWALConsumerMultiplexer::handleResult(ConsumeResult result) const
{
    /// Categorize results according to topic
    std::unordered_map<std::string, RecordPtrs> all_topic_records;

    for (auto & record : result.records)
    {
        all_topic_records[record->topic].push_back(std::move(record));
    }

    for (auto & topic_records : all_topic_records)
    {
        std::shared_ptr<std::pair<ConsumeCallback, void *>> callback;
        {
            std::lock_guard lock{callbacks_mutex};

            auto iter = callbacks.find(topic_records.first);
            assert (iter != callbacks.end());
            if (likely(iter != callbacks.end()))
            {
                callback = iter->second;
            }
        }

        if (likely(callback))
        {
            callback->first(std::move(topic_records.second), callback->second);
        }
    }
}

int32_t KafkaWALConsumerMultiplexer::commit(const TopicPartitionOffset & tpo)
{
    return consumer->commit({tpo});
}
}
