#include "DistributedWriteAheadLogKafka.h"

#include <common/logger_useful.h>

#include <librdkafka/rdkafka.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNKNOWN_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{
struct DeliveryReport
{
    Int32 partition = -1;
    Int64 offset = -1;
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
};
}

DistributedWriteAheadLogKafkaContext::DistributedWriteAheadLogKafkaContext(const String & topic_, Int32 partition_)
    : topic(topic_), partition(partition_)
{
}

std::shared_ptr<rd_kafka_topic_s> DistributedWriteAheadLogKafka::init_topic(const String & topic)
{
    using KTopicConfPtr = std::unique_ptr<rd_kafka_topic_conf_t, decltype(rd_kafka_topic_conf_destroy) *>;

    KTopicConfPtr tconf{rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy};
    if (!tconf)
    {
        LOG_ERROR(log, "KafkaWAL failed to create kafka topic conf");
        throw Exception("KafkaWAL failed to created kafka topic conf", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    String acks;
    if (settings->enable_idempotence)
         acks = "all";
     else
         std::to_string(settings->request_required_acks);

    std::vector<std::pair<String, String>> topic_params = {
        std::make_pair("request.required.acks", acks),
        /// std::make_pair("delivery.timeout.ms", std::to_string(kLocalMessageTimeout)),
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
    return std::shared_ptr<rd_kafka_topic_s>{rd_kafka_topic_new(producer_handle.get(), topic.c_str(), tconf.release()), rd_kafka_topic_destroy};
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


void DistributedWriteAheadLogKafka::delivery_report(struct rd_kafka_s *, const rd_kafka_message_s * rkmessage, void * /*opaque*/)
{
    if (rkmessage->_private)
    {
        DeliveryReport * report = static_cast<DeliveryReport *>(rkmessage->_private);
        report->err = rkmessage->err;
        report->partition = rkmessage->partition;
        report->offset = rkmessage->offset;
    }
}

DistributedWriteAheadLogKafka::DistributedWriteAheadLogKafka(std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings_)
    : settings(std::move(settings_)), log(&Poco::Logger::get("DistributedWriteAheadLogKafka")), producer_handle(nullptr, rd_kafka_destroy)
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

    std::vector<std::pair<String, String>> global_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("queue.buffering.max.messages", std::to_string(settings->queue_buffering_max_messages)),
        std::make_pair("queue.buffering.max.ms", std::to_string(settings->queue_buffering_max_ms)),
        std::make_pair("message.send.max.retries", std::to_string(settings->message_send_max_retries)),
        std::make_pair("retry.backoff.ms", std::to_string(settings->retry_backoff_ms)),
        std::make_pair("enable.idempotence", std::to_string(settings->enable_idempotence)),
        std::make_pair("compression.codec", settings->compression_codec),
        std::make_pair("statistics.interval.ms", std::to_string(settings->statistic_internal_ms)),
        std::make_pair("log_level", std::to_string(settings->log_level)),
        /// make_pair("debug", "broker,topic,msg"),
    };

    char errstr[512] = {'\0'};
    for (const auto & param : global_params)
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
    rd_kafka_conf_set_dr_msg_cb(kconf.get(), &DistributedWriteAheadLogKafka::delivery_report);
    rd_kafka_conf_set_opaque(kconf.get(), this);

    // release the ownership as rd_kafka_new takes over
    producer_handle.reset(rd_kafka_new(RD_KAFKA_PRODUCER, kconf.release(), errstr, sizeof(errstr)));
    if (!producer_handle)
    {
        LOG_ERROR(log, "KafkaWAL failed to create kafka producer, error={}", errstr);
        throw Exception("KafkaWAL failed to create kafka producer", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
}

void DistributedWriteAheadLogKafka::init_consumer()
{
    /// throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void DistributedWriteAheadLogKafka::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "KafkaWAL is stopping");

    rd_kafka_resp_err_t ret = rd_kafka_flush(producer_handle.get(), 10000);
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "KafkaWAL failed to flush kafka, timed out");
        return;
    }

    LOG_INFO(log, "KafkaWAL stopped");
}

IDistributedWriteAheadLog::AppendResult DistributedWriteAheadLogKafka::append(Record & record, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = init_topic(walctx.topic);
    }

    /// Setup deduplication ID header
    rd_kafka_headers_t * headers = rd_kafka_headers_new(1);
    const char * header_name = "_idem";
    rd_kafka_header_add(headers, header_name, std::strlen(header_name), record.idempotent_key.data(), record.idempotent_key.size());

    std::vector<UInt8> data{Record::write(record)};

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif /// __GNUC__

    DeliveryReport dr;
    int err = rd_kafka_producev(
        producer_handle.get(),
        /// Topic
        RD_KAFKA_V_RKT(walctx.topic_handle.get()),
        /// Use builtin partitioner which is consistent hashing to select partition
        RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
        /// Block if internal queue is full
        /// RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_BLOCK),
        /// Message payload and length. Note we didn't copy the data so the ownership
        /// of data is still in this function
        RD_KAFKA_V_VALUE(data.data(), data.size()),
        /// Partioner key and its length
        RD_KAFKA_V_KEY(&record.partition_key, sizeof(record.partition_key)),
        /// Headers, the memory ownership will be moved to librdkafka
        /// unless producev fails
        RD_KAFKA_V_HEADERS(headers),
        /// Message opaque, carry back the delivery report
        RD_KAFKA_V_OPAQUE(&dr),
        RD_KAFKA_V_END);

#ifdef __GNUC__
#    pragma GCC diagnostic pop
#endif  /// __GNUC__

    if (err)
    {
        /// failed to call `producev`, headers's memory ownership was not moved, release it
        rd_kafka_headers_destroy(headers);
        handle_error(err, record, walctx);
    }
    else
    {
        while (true)
        {
            /// FIXME, we need continously poll rk to drain the report
            /// poll interval or non-blocking poll
            /// We wait for poll.max milliseconds for the message to get delivered
            rd_kafka_poll(producer_handle.get(), settings->message_delivery_poll_max_ms);
            if (dr.offset != -1)
            {
                return {.sn = dr.offset, .ctx = dr.partition};
            }
        }

        LOG_ERROR(log, "KafkaWal produce message timed out, topic={} partition_key={}", walctx.topic, record.partition_key);
        throw Exception("KafkaWal produce message timed out", ErrorCodes::TIMEOUT_EXCEEDED);
    }
    __builtin_unreachable();
}

void DistributedWriteAheadLogKafka::handle_error(int err, const Record & record, const DistributedWriteAheadLogKafkaContext & ctx)
{
    auto kerr = static_cast<rd_kafka_resp_err_t>(err);
    LOG_ERROR(
        log,
        "KafkaWal failed to write record to topic={} partition_key={} error={}",
        ctx.topic,
        record.partition_key,
        rd_kafka_err2str(kerr));

    if (kerr == RD_KAFKA_RESP_ERR__FATAL)
    {
        /// for fatal error, we need replace this KafkaWal instance with
        /// a new one
    }
    else if (kerr == RD_KAFKA_RESP_ERR__QUEUE_FULL)
    {
        /// Server busy
    }
    else
    {
    }
    throw Exception("KafakaWal failed to write record", ErrorCodes::UNKNOWN_EXCEPTION);
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
