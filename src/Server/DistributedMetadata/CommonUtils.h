#pragma once

#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <Poco/Logger.h>

#include <any>


namespace DB
{
void createDWal(DistributedWriteAheadLogPtr & dwal, std::any & ctx, std::atomic_flag & stopped, Poco::Logger * log);
}
