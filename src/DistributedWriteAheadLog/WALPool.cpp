#include "WALPool.h"

#include <DistributedWriteAheadLog/KafkaWAL.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DWAL
{
namespace
{
    /// Globals
    const String SYSTEM_WALS_KEY = "cluster_settings.streaming_storage";
    const String SYSTEM_WALS_KEY_PREFIX = "cluster_settings.streaming_storage.";
}

WALPool & WALPool::instance(ContextPtr global_context)
{
    static WALPool pool{global_context};
    return pool;
}

WALPool::WALPool(ContextPtr global_context_)
    : global_context(global_context_), log(&Poco::Logger::get("WALPool"))
{
}

WALPool::~WALPool()
{
    shutdown();
}

void WALPool::startup()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    const auto & config = global_context->getConfigRef();

    Poco::Util::AbstractConfiguration::Keys sys_WAL_keys;
    config.keys(SYSTEM_WALS_KEY, sys_WAL_keys);

    for (const auto & key : sys_WAL_keys)
    {
        init(key);
    }

    if (!wals.empty() && default_cluster.empty())
    {
        throw Exception("Default Kafka WAL cluster is not assigned", ErrorCodes::BAD_ARGUMENTS);
    }

    LOG_INFO(log, "Started");
}

void WALPool::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");

    for (auto & cluster_wals : wals)
    {
        for (auto & wal : cluster_wals.second)
        {
            wal->shutdown();
        }
    }

    if (meta_wal)
    {
        meta_wal->shutdown();
    }

    {
        std::lock_guard lock{streaming_wals_lock};
        streaming_wals.clear();
    }

    LOG_INFO(log, "Stopped");
}

void WALPool::init(const String & key)
{
    /// FIXME; for now, we only support kafka, so assume it is kafka
    /// assert(key.startswith("kafka"));

    const auto & config = global_context->getConfigRef();

    KafkaWALSettings kafka_settings;
    Int32 wal_pool_size = 0;
    bool system_default = false;

    std::vector<std::tuple<String, String, void *>> settings = {
        {".default", "Bool", &system_default},
        {".cluster_id", "String", &kafka_settings.cluster_id},
        {".security_protocol", "String", &kafka_settings.security_protocol},
        {".brokers", "String", &kafka_settings.brokers},
        {".topic_metadata_refresh_interval_ms", "Int32", &kafka_settings.topic_metadata_refresh_interval_ms},
        {".message_max_bytes", "Int32", &kafka_settings.message_max_bytes},
        {".statistic_internal_ms", "Int32", &kafka_settings.statistic_internal_ms},
        {".debug", "String", &kafka_settings.debug},
        {".enable_idempotence", "Bool", &kafka_settings.enable_idempotence},
        {".queue_buffering_max_messages", "Int32", &kafka_settings.queue_buffering_max_messages},
        {".queue_buffering_max_kbytes", "Int32", &kafka_settings.queue_buffering_max_kbytes},
        {".queue_buffering_max_ms", "Int32", &kafka_settings.queue_buffering_max_ms},
        {".message_send_max_retries", "Int32", &kafka_settings.message_send_max_retries},
        {".retry_backoff_ms", "Int32", &kafka_settings.retry_backoff_ms},
        {".compression_codec", "String", &kafka_settings.compression_codec},
        {".message_timeout_ms", "Int32", &kafka_settings.message_timeout_ms},
        {".message_delivery_async_poll_ms", "Int32", &kafka_settings.message_delivery_async_poll_ms},
        {".message_delivery_sync_poll_ms", "Int32", &kafka_settings.message_delivery_sync_poll_ms},
        {".group_id", "String", &kafka_settings.group_id},
        {".message_max_bytes", "Int32", &kafka_settings.message_max_bytes},
        {".enable_auto_commit", "Bool", &kafka_settings.enable_auto_commit},
        {".check_crcs", "Bool", &kafka_settings.check_crcs},
        {".auto_commit_interval_ms", "Int32", &kafka_settings.auto_commit_interval_ms},
        {".fetch_message_max_bytes", "Int32", &kafka_settings.fetch_message_max_bytes},
        {".queued_min_messages", "Int32", &kafka_settings.queued_min_messages},
        {".queued_max_messages_kbytes", "Int32", &kafka_settings.queued_max_messages_kbytes},
        {".internal_pool_size", "Int32", &wal_pool_size},
    };

    for (const auto & t : settings)
    {
        auto k = SYSTEM_WALS_KEY_PREFIX + key + std::get<0>(t);
        if (config.has(k))
        {
            const auto & type = std::get<1>(t);
            if (type == "String")
            {
                *static_cast<String *>(std::get<2>(t)) = config.getString(k);
            }
            else if (type == "Int32")
            {
                auto i = config.getInt(k);
                if (i <= 0)
                {
                    throw Exception("Invalid setting " + std::get<0>(t), ErrorCodes::BAD_ARGUMENTS);
                }
                *static_cast<Int32 *>(std::get<2>(t)) = i;
            }
            else if (type == "Bool")
            {
                *static_cast<bool *>(std::get<2>(t)) = config.getBool(k);
            }
        }
    }

    if (kafka_settings.brokers.empty())
    {
        LOG_ERROR(log, "Invalid system kafka settings, empty brokers, will skip settings in this segment");
        return;
    }

    if (kafka_settings.group_id.empty())
    {
        /// FIXME
        kafka_settings.group_id = global_context->getNodeIdentity();
    }

    if (wals.contains(kafka_settings.cluster_id))
    {
        throw Exception("Duplicated Kafka cluster id " + kafka_settings.cluster_id, ErrorCodes::BAD_ARGUMENTS);
    }

    /// Create WALs
    LOG_INFO(log, "Creating Kafka WAL with settings: {}", kafka_settings.string());

    for (Int32 i = 0; i < wal_pool_size; ++i)
    {
        auto kwal
            = std::make_shared<KafkaWAL>(std::make_unique<KafkaWALSettings>(kafka_settings));

        kwal->startup();
        wals[kafka_settings.cluster_id].push_back(kwal);

    }
    indexes[kafka_settings.cluster_id] = 0;

    if (system_default)
    {
        LOG_INFO(log, "Setting {} cluster as default Kafka WAL cluster", kafka_settings.cluster_id);
        default_cluster = kafka_settings.cluster_id;

        /// Meta WAL with a different consumer group
        kafka_settings.group_id += "-meta";
        meta_wal = std::make_shared<KafkaWAL>(std::make_unique<KafkaWALSettings>(kafka_settings));
        meta_wal->startup();
    }
}

WALPtr WALPool::get(const String & id) const
{
    if (id.empty() && !default_cluster.empty())
    {
        return get(default_cluster);
    }

    auto iter = wals.find(id);
    if (iter == wals.end())
    {
        return nullptr;
    }

    return iter->second[indexes[id]++ % iter->second.size()];
}

WALPtr WALPool::getOrCreateStreaming(const String & id, const String & cluster_id)
{
    std::lock_guard lock{streaming_wals_lock};

    auto iter = streaming_wals.find(id);
    if (iter != streaming_wals.end())
    {
        return iter->second;
    }

    const String * cid = &cluster_id;
    if (cluster_id.empty())
    {
        cid = &default_cluster;
    }

    /// Create one
    auto wal_iter = wals.find(*cid);
    assert (wal_iter != wals.end());
    if (wal_iter != wals.end())
    {
        auto ksettings{static_cast<KafkaWAL *>(wal_iter->second.back().get())->getSettings()};

        /// We don't care offset checkpointing for WALs used for streaming processing,
        /// and we only need consumer

        /// Streaming WALs have a different group ID
        ksettings->group_id = id + "-streaming";
        ksettings->mode_producer_consumer = KafkaWALSettings::EProducerConsumer::CONSUMER_ONLY;
        /// No auto commit
        ksettings->enable_auto_commit = false;

        auto kwal = std::make_shared<KafkaWAL>(std::move(ksettings));
        kwal->startup();
        streaming_wals[id] = kwal;
        return kwal;
    }
    return nullptr;
}

void WALPool::deleteStreaming(const String & id)
{
    std::lock_guard lock{streaming_wals_lock};

    streaming_wals.erase(id);
}

WALPtr WALPool::getMeta() const { return meta_wal; }

std::vector<ClusterPtr> WALPool::clusters(std::any & ctx) const
{
    std::vector<ClusterPtr> results;
    results.reserve(wals.size());

    for (const auto & cluster_wal : wals)
    {
        auto result{cluster_wal.second.back()->cluster(ctx)};
        if (result)
        {
            results.push_back(std::move(result));
        }
    }

    return results;
}
}
}
