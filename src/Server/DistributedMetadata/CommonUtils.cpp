#include "CommonUtils.h"

#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

/// try indefinitely to create dwal for catalog
void createDWal(DistributedWriteAheadLogPtr & dwal, std::any & ctx, std::atomic_flag & stopped, Poco::Logger * log)
{
    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (dwal->describe(kctx.topic, ctx) == ErrorCodes::OK)
    {
        return;
    }

    LOG_INFO(log, "Didn't find topic={}, create one", kctx.topic);
    while (!stopped.test())
    {
        if (dwal->create(kctx.topic, ctx) == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully created topic={}", kctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to create topic={}, will retry indefinitely ...", kctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}
}
