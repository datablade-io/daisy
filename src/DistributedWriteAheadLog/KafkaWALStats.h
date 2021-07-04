#pragma once

#include <atomic>
#include <string>
#include <memory>

namespace Poco
{
    class Logger;
}

namespace DWAL
{
struct KafkaWALStats
{
    std::atomic_uint64_t received = 0;
    std::atomic_uint64_t dropped = 0;
    std::atomic_uint64_t failed = 0;
    std::atomic_uint64_t bytes = 0;

    /// Produce statistics
    std::string pstat;

    Poco::Logger * log;

    explicit KafkaWALStats(Poco::Logger * log_) : log(log_) { }
};

using KafkaWALStatsPtr = std::unique_ptr<KafkaWALStats>;
}
