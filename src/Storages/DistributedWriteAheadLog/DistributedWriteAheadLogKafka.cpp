#include "DistributedWriteAheadLogKafka.h"

#include <Common/Macros.h>
#include <Common/config_version.h>
#include <common/logger_useful.h>

#include <librdkafka/rdkafka.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
const int kStatsInterval = 30000;  // in milliseconds
const int kQueueMaxMsg = 100000;
const int kQueueMaxKBytes = 1000000;
const int kQueueMaxLingerTime = 500;  // in milliseconds
// local msg timeout, librdkafka will wait for
// max period of this time. In milliseconds
const int kLocalMessageTimeout = 30000;
}

DistributedWriteAheadLogKafkaContext::DistributedWriteAheadLogKafkaContext(const String & topic_, Int64 partition_, UInt64 partition_key_)
    : topic(topic_), partition(partition_), partition_key(partition_key_)
{
}

std::shared_ptr<rd_kafka_topic_s> DistributedWriteAheadLogKafka::init_topic(const String & topic)
{
    using KTopicConfPtr = std::shared_ptr<rd_kafka_topic_conf_t>;

    KTopicConfPtr tconf{rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy};
    if (!tconf)
    {
        LOG_ERROR(log, "KafkaWAL failed to create kafka topic conf");
        throw Exception("KafkaWAL failed to created kafka topic conf", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    std::vector<std::pair<String, String>> topic_params = {
        std::make_pair("request.required.acks", std::to_string(1)),
        std::make_pair("delivery.timeout.ms", std::to_string(kLocalMessageTimeout)),
        /// FIXME, partitioner
        std::make_pair("partitioner", "consistent_random"),
        std::make_pair("compression.codec", "inherit"),
    };

    char errstr[512] = {'\0'};
    for (const auto & param : topic_params)
    {
        auto ret = rd_kafka_topic_conf_set(tconf.get(), param.first.c_str(), param.second.c_str(),
                                           errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(log, "KafkaWAL failed to set kafka {}={} conf", param.first, param.second);
            throw Exception("KafkaWAL failed to set kafka", ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    /// rd_kafka_topic_conf_set_partitioner_cb;
    /// rd_kafka_topic_conf_set_opaque(tconf.get(), this);

    /// release the ownership as `rd_kafka_topic_new` takes over
    return std::shared_ptr<rd_kafka_topic_s>{rd_kafka_topic_new(rd_kafka_handle.get(), topic.c_str(), tconf.get()), rd_kafka_topic_destroy};
}

void DistributedWriteAheadLogKafka::log_failed_msg(struct rd_kafka_s * rk, const struct rd_kafka_message_s * msg, void * opaque)
{
    DistributedWriteAheadLogKafka* k = static_cast<DistributedWriteAheadLogKafka*>(opaque);
    k->log_failed_msg(rk, msg);
}

inline void DistributedWriteAheadLogKafka::log_failed_msg(struct rd_kafka_s *, const struct rd_kafka_message_s * msg)
{
    if (msg->err)
    {
        LOG_ERROR(log, "KafkaWAL failed to delivery message, error={}", rd_kafka_err2str(msg->err));
        failed += 1;
    }
}

int DistributedWriteAheadLogKafka::log_stats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque)
{
    DistributedWriteAheadLogKafka * k = static_cast<DistributedWriteAheadLogKafka *>(opaque);
    k->log_stats(rk, json, json_len);
    return 0;
}

inline void DistributedWriteAheadLogKafka::log_stats(struct rd_kafka_s *, char * json, size_t json_len)
{
    String stat(json, json + json_len);
    pstat.swap(stat);
}

void DistributedWriteAheadLogKafka::log_err(struct rd_kafka_s * rk, int err, const char * reason, void * opaque)
{
    DistributedWriteAheadLogKafka * k = static_cast<DistributedWriteAheadLogKafka*>(opaque);
    k->log_err(rk, err, reason);
}

inline void DistributedWriteAheadLogKafka::log_err(struct rd_kafka_s * rk, int err, const char * reason)
{
    if (err == RD_KAFKA_RESP_ERR__FATAL)
    {
        char errstr[512] = {'\0'};
        rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        LOG_ERROR(log, "KafkaWAL fatal error found, error={}", errstr);
    }
    else
    {
        LOG_WARNING(log, "KafkaWAL error found, error={}, reason={}", rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)), reason);
    }
}

void DistributedWriteAheadLogKafka::log_throttle(
    struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque)
{
    DistributedWriteAheadLogKafka * k = static_cast<DistributedWriteAheadLogKafka *>(opaque);
    k->log_throttle(rk, broker_name, broker_id, throttle_time_ms);
}

inline void
DistributedWriteAheadLogKafka::log_throttle(struct rd_kafka_s *, const char * broker_name, int32_t broker_id, int throttle_time_ms)
{
    LOG_WARNING(
        log, "KafkaWAL throttling found on broker={}, broker_id={}, throttle_time_ms={}", broker_name, broker_id, throttle_time_ms);
}

DistributedWriteAheadLogKafka::DistributedWriteAheadLogKafka(std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings_)
    : settings(std::move(settings_)), log(&Poco::Logger::get("DistributedWriteAheadLogKafka")), rd_kafka_handle(nullptr, rd_kafka_destroy)
{
}

DistributedWriteAheadLogKafka::~DistributedWriteAheadLogKafka()
{
    shutdown();
}

void DistributedWriteAheadLogKafka::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "KafkaWAL has already started");
        return;
    }

    init_producer();
    init_consumer();
}

void DistributedWriteAheadLogKafka::init_producer()
{
    using KConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;

    KConfPtr kconf{rd_kafka_conf_new(), rd_kafka_conf_destroy};
    if (!kconf)
    {
        LOG_ERROR(log, "KafkaWAL failed to create kafka conf");
        throw Exception("KafkaWAL failed to create kafka conf", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    /// FIXME, other system settings
    std::vector<std::pair<String, String>> global_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("statistics.interval.ms", std::to_string(kStatsInterval)),
        std::make_pair("log_level", std::to_string(6)),
        std::make_pair("queue.buffering.max.messages", std::to_string(kQueueMaxMsg)),
        std::make_pair("queue.buffering.max.kbytes", std::to_string(kQueueMaxKBytes)),
        std::make_pair("queue.buffering.max.ms", std::to_string(kQueueMaxLingerTime)),
        std::make_pair("message.send.max.retries", std::to_string(settings->retries)),
        /// make_pair("compression.codec", conf_.compression_),
        /// make_pair("debug", "broker,topic,msg"),
    };

    char errstr[512] = {'\0'};
    for (const auto& param : global_params)
    {
        auto ret = rd_kafka_conf_set(kconf.get(), param.first.c_str(), param.second.c_str(), errstr,
                                     sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(log, "KafkaWAL failed to set kafka {}={} conf", param.first, param.second);
            throw Exception("KafkaWAL failed to create kafka conf", ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

#ifndef NDEBUG
    rd_kafka_conf_set_dr_msg_cb(kconf.get(), &DistributedWriteAheadLogKafka::log_failed_msg);
#endif

    rd_kafka_conf_set_stats_cb(kconf.get(), &DistributedWriteAheadLogKafka::log_stats);
    rd_kafka_conf_set_error_cb(kconf.get(), &DistributedWriteAheadLogKafka::log_err);
    rd_kafka_conf_set_throttle_cb(kconf.get(), &DistributedWriteAheadLogKafka::log_throttle);
    rd_kafka_conf_set_opaque(kconf.get(), this);

    // release the ownership as rd_kafka_new takes over
    rd_kafka_handle.reset(rd_kafka_new(RD_KAFKA_PRODUCER, kconf.release(), errstr, sizeof(errstr)));
    if (!rd_kafka_handle)
    {
        LOG_ERROR(log, "KafkaWAL failed to create kafka producer, error={}", errstr);
        throw Exception("KafkaWAL failed to create kafka producer", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
}

void DistributedWriteAheadLogKafka::init_consumer()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void DistributedWriteAheadLogKafka::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "KafkaWAL is stopping");

    rd_kafka_resp_err_t ret = rd_kafka_flush(rd_kafka_handle.get(), 10000);
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "KafkaWAL failed to flush kafka, timed out");
        return;
    }

    LOG_INFO(log, "KafkaWAL stopped");
}

IDistributedWriteAheadLog::RecordSequenceNumber DistributedWriteAheadLogKafka::append(Record & record, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = init_topic(walctx.topic);
    }

    std::vector<UInt8> data{Record::serialize(record)};

    (void)data;
    return {};
}

IDistributedWriteAheadLog::Records DistributedWriteAheadLogKafka::consume(size_t count, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = init_topic(walctx.topic);
    }

    (void)count;
    return {};
}

void DistributedWriteAheadLogKafka::commit(const IDistributedWriteAheadLog::RecordSequenceNumbers & sequence_numbers, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = init_topic(walctx.topic);
    }

    (void)sequence_numbers;
}
}
