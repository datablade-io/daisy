#include "TaskStatusService.h"

#include <Interpreters/Context.h>
#include <common/logger_useful.h>


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

const String TaskStatusService::TaskStatus::SUBMITTED = "SUBMITTED";
const String TaskStatusService::TaskStatus::INPROGRESS = "INPROGRESS";
const String TaskStatusService::TaskStatus::SUCCEEDED = "SUCCEEDED";
const String TaskStatusService::TaskStatus::FAILED = "FAILED";


TaskStatusService & TaskStatusService::instance(Context & global_context_)
{
    static TaskStatusService task_status_service{global_context_};
    return task_status_service;
}

TaskStatusService::TaskStatusService(Context & global_context_) : MetadataService(global_context_, "TaskStatusService")
{
}

MetadataService::ConfigSettings TaskStatusService::configSettings() const
{
    return {
        .name_key = TASK_NAME_KEY,
        .default_name = TASK_DEFAULT_TOPIC,
        .data_retention_key = TASK_DATA_RETENTION_KEY,
        .default_data_retention = 24,
        .replication_factor_key = TASK_REPLICATION_FACTOR_KEY,
        .auto_offset_reset = "earliest",
    };
}

Int32 TaskStatusService::append(TaskStatusPtr task)
{
    (void)task;
    return 0;
}

void TaskStatusService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    (void)records;
}
}
