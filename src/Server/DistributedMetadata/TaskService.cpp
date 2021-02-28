#include "TaskService.h"

#include <common/logger_useful.h>

#include <Interpreters/Context.h>


namespace DB
{
namespace
{
String TASK_KEY_PREFIX = "system_settings.system_task_dwal.";
String TASK_NAME_KEY = TASK_KEY_PREFIX + "name";
String TASK_REPLICATION_FACTOR_KEY = TASK_KEY_PREFIX + "replication_factor";
String TASK_DATA_RETENTION_KEY = TASK_KEY_PREFIX + "data_retention";
String TASK_DEFAULT_TOPIC = "__system_tasks";
}

TaskService & TaskService::instance(Context & global_context)
{
    static TaskService task_service{global_context};
    return task_service;
}

TaskService::TaskService(Context & global_context_) : MetadataService(global_context_, "TaskService")
{
}

MetadataService::ConfigSettings TaskService::configSettings() const
{
    return {
        .name_key = TASK_NAME_KEY,
        .default_name = TASK_DEFAULT_TOPIC,
        .data_retention_key = TASK_DATA_RETENTION_KEY,
        .default_data_retention = 168,
        .replication_factor_key = TASK_REPLICATION_FACTOR_KEY,
        .auto_offset_reset = "earliest",
    };
}

void TaskService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    (void)records;
}
}
