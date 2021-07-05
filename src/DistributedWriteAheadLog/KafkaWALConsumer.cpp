#include "KafkaWALConsumer.h"
#include "KafkaWALCommon.h"

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_NOT_INITED;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int DWAL_FATAL_ERROR;
}
}

namespace DWAL
{
KafkaWALConsumer::KafkaWALConsumer(std::unique_ptr<KafkaWALSettings> settings_)
    : settings(std::move(settings_))
    , consumer_handle(nullptr, rd_kafka_destroy)
    , poller(1)
    , log(&Poco::Logger::get("KafkaWALConsumer"))
    , stats(std::make_unique<KafkaWALStats>(log, "consumer"))
{
}

KafkaWALConsumer::~KafkaWALConsumer()
{
    shutdown();
}

void KafkaWALConsumer::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initHandle();

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });

    LOG_INFO(log, "Started");
}

void KafkaWALConsumer::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    poller.wait();
    LOG_INFO(log, "Stopped");
}

void KafkaWALConsumer::backgroundPoll()
{
    LOG_INFO(log, "Polling consumer started");
    setThreadName("KWalCPoller");

    while (!stopped.test())
    {
        rd_kafka_poll(consumer_handle.get(), 100);
    }

    auto err = rd_kafka_commit(consumer_handle.get(), nullptr, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__NO_OFFSET)
    {
        LOG_ERROR(log, "Failed to commit offsets, error={}", rd_kafka_err2str(err));
    }

    LOG_INFO(log, "Polling consumer stopped");
}

void KafkaWALConsumer::initHandle()
{
    /// 1) use high level Kafka consumer
    /// 2) manually manage offset commit
    /// 3) offsets are stored in brokers and on application side.
    ///     3.1) In normal cases, application side offsets overrides offsets in borkers
    ///     3.2) In corruption cases (application offsets corruption), use offsets in borkers
    std::vector<std::pair<std::string, std::string>> consumer_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("group.id", settings->group_id),
        /// enable auto offset commit
        std::make_pair("enable.auto.commit", "true"),
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)),
        std::make_pair("fetch.message.max.bytes", std::to_string(settings->fetch_message_max_bytes)),
        std::make_pair("enable.auto.offset.store", "false"),
        std::make_pair("offset.store.method", "broker"),
        std::make_pair("enable.partition.eof", "false"),
        std::make_pair("queued.min.messages", std::to_string(settings->queued_min_messages)),
        std::make_pair("queued.max.messages.kbytes", std::to_string(settings->queued_max_messages_kbytes)),
        /// incremental partition assignment / unassignment
        std::make_pair("partition.assignment.strategy", "cooperative-sticky"),
        /// consumer group membership heartbeat timeout
        std::make_pair("session.timeout.ms", std::to_string(settings->session_timeout_ms)),
        std::make_pair("max.poll.interval.ms", std::to_string(settings->max_poll_interval_ms)),
    };

    if (!settings->debug.empty())
    {
        consumer_params.emplace_back("debug", settings->debug);
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_conf_set_stats_cb(kconf, &KafkaWALStats::logStats);
        rd_kafka_conf_set_error_cb(kconf, &KafkaWALStats::logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &KafkaWALStats::logThrottle);

        /// Consumer offset commits
        /// rd_kafka_conf_set_offset_commit_cb(kconf, &KafkaWALStats::logOffsetCommits);
    };

    consumer_handle = initRdKafkaHandle(RD_KAFKA_CONSUMER, consumer_params, stats.get(), cb_setup);

    /// Forward all events to consumer queue. there may have in-balance consuming problems
    /// rd_kafka_poll_set_consumer(consumer_handle.get());
}

int32_t KafkaWALConsumer::addConsumptions(const TopicPartionOffsets & partitions_)
{
    std::lock_guard lock{partitions_mutex};

    for (const auto & partition : partitions_)
    {
        auto iter = partitions.find(partition.topic);
        if (iter != partitions.end())
        {
            auto pos = std::find(iter->second.begin(), iter->second.end(), partition.partition);
            if (pos != iter->second.end())
            {
                return DB::ErrorCodes::BAD_ARGUMENTS;
            }
        }
    }

    auto topic_partitions = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>(
        rd_kafka_topic_partition_list_new(partitions_.size()), rd_kafka_topic_partition_list_destroy);

    for (const auto & partition : partitions_)
    {
        auto new_partition = rd_kafka_topic_partition_list_add(topic_partitions.get(), partition.topic.c_str(), partition.partition);
        new_partition->offset = partition.offset;
    }

    auto err = rd_kafka_incremental_assign(consumer_handle.get(), topic_partitions.get());
    if (err)
    {
        LOG_ERROR(log, "Failed to assign partitions incrementally, error={}", rd_kafka_error_string(err));

        auto ret_code = mapErrorCode(rd_kafka_error_code(err), rd_kafka_error_is_retriable(err));
        rd_kafka_error_destroy(err);
        return ret_code;
    }

    for (const auto & partition : partitions_)
    {
        partitions[partition.topic].push_back(partition.partition);
    }
    return DB::ErrorCodes::OK;
}

int32_t KafkaWALConsumer::removeConsumptions(const TopicPartionOffsets & partitions_)
{
    std::lock_guard lock{partitions_mutex};

    for (const auto & partition : partitions_)
    {
        auto iter = partitions.find(partition.topic);
        if (iter == partitions.end())
        {
            return 1;
        }

        auto pos = std::find(iter->second.begin(), iter->second.end(), partition.partition);
        if (pos == iter->second.end())
        {
            return DB::ErrorCodes::BAD_ARGUMENTS;
        }
    }

    auto topic_partitions = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>(
        rd_kafka_topic_partition_list_new(partitions_.size()), rd_kafka_topic_partition_list_destroy);

    for (const auto & partition : partitions_)
    {
        rd_kafka_topic_partition_list_add(topic_partitions.get(), partition.topic.c_str(), partition.partition);
    }

    auto err = rd_kafka_incremental_unassign(consumer_handle.get(), topic_partitions.get());
    if (err)
    {
        LOG_ERROR(log, "Failed to unassign partitions incrementally, error={}", rd_kafka_error_string(err));

        auto ret_code = mapErrorCode(rd_kafka_error_code(err), rd_kafka_error_is_retriable(err));
        rd_kafka_error_destroy(err);
        return ret_code;
    }

    for (const auto & partition : partitions_)
    {
        auto iter = partitions.find(partition.topic);
        assert(iter != partitions.end());

        auto pos = std::find(iter->second.begin(), iter->second.end(), partition.partition);
        assert(pos != iter->second.end());
        iter->second.erase(pos);

        if (iter->second.empty())
        {
            partitions.erase(iter);
        }
    }

    return DB::ErrorCodes::OK;
}

ConsumeResult KafkaWALConsumer::consume(int32_t timeout_ms)
{
    auto rkmessage = rd_kafka_consumer_poll(consumer_handle.get(), timeout_ms);
    if (rkmessage)
    {
        if (likely(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR))
        {
            /*if (rkmessage->offset < wrapped->ctx.offset)
            {
                /// Ignore the message which has lower offset than what clients like to have
                return;
            }*/
            auto record = kafkaMsgToRecord(rkmessage);
            return {.err = DB::ErrorCodes::OK, .records = {std::move(record)}};
        }
        else
        {
            return {.err = mapErrorCode(rkmessage->err)};
        }
    }
    return {.err = DB::ErrorCodes::OK};
};
}
