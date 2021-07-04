#include "MordenKafkaWAL.h"

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

#include <librdkafka/rdkafka.h>


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

namespace DWAL
{
namespace
{
    /// types
    using KConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;
    using KTopicConfPtr = std::unique_ptr<rd_kafka_topic_conf_t, decltype(rd_kafka_topic_conf_destroy) *>;
    using KConfCallback = std::function<void(rd_kafka_conf_t *)>;
    using KConfParams = std::vector<std::pair<String, String>>;

    Int32 mapErrorCode(rd_kafka_resp_err_t err)
    {
        /// FIXME, more code mapping
        switch (err)
        {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return ErrorCodes::OK;

            case RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS:
                return ErrorCodes::RESOURCE_ALREADY_EXISTS;

            case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
                /// fallthrough
            case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
                /// fallthrough
            case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                return ErrorCodes::RESOURCE_NOT_FOUND;

            case RD_KAFKA_RESP_ERR__INVALID_ARG:
                return ErrorCodes::BAD_ARGUMENTS;

            case RD_KAFKA_RESP_ERR__FATAL:
                throw Exception("Fatal error occurred, shall tear down the whole program", ErrorCodes::DWAL_FATAL_ERROR);

            default:
                return ErrorCodes::UNKNOWN_EXCEPTION;
        }
}



    std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)>
    initRdKafkaHandle(rd_kafka_type_t type, KConfParams & params, MordenKafkaWAL::StatsPtr stats, KConfCallback cb_setup)
    {
        KConfPtr kconf{rd_kafka_conf_new(), rd_kafka_conf_destroy};
        if (!kconf)
        {
            LOG_ERROR(stats->log, "Failed to create kafka conf, error={}", rd_kafka_err2str(rd_kafka_last_error()));
            throw Exception("Failed to create kafka conf", mapErrorCode(rd_kafka_last_error()));
        }

        char errstr[512] = {'\0'};
        for (const auto & param : params)
        {
            auto ret = rd_kafka_conf_set(kconf.get(), param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
            if (ret != RD_KAFKA_CONF_OK)
            {
                LOG_ERROR(stats->log, "Failed to set kafka param_name={} param_value={} error={}", param.first, param.second, ret);
                throw Exception("Failed to create kafka conf", ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
        }

        if (cb_setup)
        {
            cb_setup(kconf.get());
        }

        rd_kafka_conf_set_opaque(kconf.get(), stats.get());

        std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)> kafka_handle(
            rd_kafka_new(type, kconf.release(), errstr, sizeof(errstr)), rd_kafka_destroy);
        if (!kafka_handle)
        {
            LOG_ERROR(stats->log, "Failed to create kafka handle, error={}", errstr);
            throw Exception("Failed to create kafka handle", mapErrorCode(rd_kafka_last_error()));
        }

        return kafka_handle;
    }

    inline RecordPtr kafkaMsgToRecord(rd_kafka_message_t * msg)
    {
        assert(msg != nullptr);

        auto record = Record::read(static_cast<const char *>(msg->payload), msg->len);
        if (unlikely(!record))
        {
            return nullptr;
        }

        record->sn = msg->offset;
        record->partition_key = msg->partition;
        record->topic = rd_kafka_topic_name(msg->rkt);

        rd_kafka_headers_t * hdrs = nullptr;
        if (rd_kafka_message_headers(msg, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            /// Has headers
            auto n = rd_kafka_header_cnt(hdrs);
            for (size_t i = 0; i < n; ++i)
            {
                const char * name = nullptr;
                const void * value = nullptr;
                size_t size = 0;

                if (rd_kafka_header_get_all(hdrs, i, &name, &value, &size) == RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    record->headers.emplace(name, String{static_cast<const char *>(value), size});
                }
            }
        }

        return record;
}
}

MordenKafkaWAL::MordenKafkaWAL(std::unique_ptr<KafkaWALSettings> settings_)
    : settings(std::move(settings_))
    , consumer_handle(nullptr, rd_kafka_destroy)
    , log(&Poco::Logger::get("MordenKafkaWAL"))
    , stats(std::make_shared<Stats>(log))
{
}

MordenKafkaWAL::~MordenKafkaWAL()
{
    shutdown();
}

void MordenKafkaWAL::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initConsumer();

    /// poller.scheduleOrThrowOnError([this] { backgroundPollConsumer(); });

    LOG_INFO(log, "Started");
}

void MordenKafkaWAL::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    /// poller.wait();
    LOG_INFO(log, "Stopped");
}

void MordenKafkaWAL::initConsumer()
{
    /// 1) use high level Kafka consumer
    /// 2) manually manage offset commit
    /// 3) offsets are stored in brokers and on application side.
    ///     3.1) In normal cases, application side offsets overrides offsets in borkers
    ///     3.2) In corruption cases (application offsets corruption), use offsets in borkers
    std::vector<std::pair<String, String>> consumer_params = {
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
        /// std::make_pair("session.timeout.ms", ""),
    };

    if (!settings->debug.empty())
    {
        consumer_params.emplace_back("debug", settings->debug);
    }

    /* auto cb_setup = [](rd_kafka_conf_t * kconf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_conf_set_stats_cb(kconf, &logStats);
        rd_kafka_conf_set_error_cb(kconf, &logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &logThrottle);

        /// offset commits
        rd_kafka_conf_set_offset_commit_cb(kconf, &logOffsetCommits);
    }; */

    consumer_handle = initRdKafkaHandle(RD_KAFKA_CONSUMER, consumer_params, stats, nullptr);

    /// Forward all events to consumer queue. there may have in-balance consuming problems
    rd_kafka_poll_set_consumer(consumer_handle.get());
}

Int32 MordenKafkaWAL::addConsumptions(const TopicPartionOffsets & partitions_)
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
                /// FIXME
                return 1;
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
        rd_kafka_error_destroy(err);
        return 1;
    }

    for (const auto & partition : partitions_)
    {
        partitions[partition.topic].push_back(partition.partition);
    }
    return 0;
}

Int32 MordenKafkaWAL::removeConsumptions(const TopicPartionOffsets & partitions_)
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
            /// FIXME
            return 1;
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
        rd_kafka_error_destroy(err);
        return 1;
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

    return 0;
}

WAL::ConsumeResult MordenKafkaWAL::consume(Int32 timeout_ms)
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
            return {.err = ErrorCodes::OK, .records = {std::move(record)}};
        }
        else
        {
            return {.err = mapErrorCode(rkmessage->err)};
        }
    }
    return {.err = ErrorCodes::OK};
};
}
}
