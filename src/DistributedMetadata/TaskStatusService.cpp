#include "TaskStatusService.h"

#include <common/DateLUT.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <fmt/format.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <common/logger_useful.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <chrono>
#include <vector>
#include <mutex>

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
    /// TODO: Add a table in DB if it is not exist for storing task
}

MetadataService::ConfigSettings TaskStatusService::configSettings() const
{
    return {
        .name_key = TASK_NAME_KEY,
        .default_name = TASK_DEFAULT_TOPIC,
        .data_retention_key = TASK_DATA_RETENTION_KEY,
        .default_data_retention = 24,
        .replication_factor_key = TASK_REPLICATION_FACTOR_KEY,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

Int32 TaskStatusService::append(TaskStatusPtr task)
{
   auto record_ptr = getRecordFromTaskStatus(task);
   dwal->append(*record_ptr, dwal_append_ctx);
   return 0;
}

void TaskStatusService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    std::unique_lock guard(rwlock);
    /// Consumer records and put them into memory
    for(auto & record : records)
    {
        if(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK)
        {
            auto task_ptr = getTaskStatusFromRecord(record);
            auto it_id_map = indexedById.find(task_ptr->id);
            if(it_id_map != indexedById.end())
            {
                task_ptr->created = it_id_map->second->created;
            }

            indexedById[task_ptr->id] = task_ptr;

            /// Couldn't find tasks list by user
            auto it_user_map = indexedByUser.find(task_ptr->user);
            if(it_user_map == indexedByUser.end())
            {
                indexedByUser[task_ptr->user] = std::vector<TaskStatusPtr>{task_ptr};
                continue;
            }

            /// Found task list by user
            auto & tasks = indexedByUser[task_ptr->user];
            bool found_task = false;
            for(auto & task : tasks)
            {
                if(task_ptr->id == task->id)
                {
                    /// Task is in the task list, update it
                    task = task_ptr;
                    found_task = true;
                    break;
                }
            }

            /// Task is not in the task list, append it to the task list
            if(!found_task)
            {
                it_user_map->second.push_back(task_ptr);
            }
        }
    }
}

IDistributedWriteAheadLog::RecordPtr TaskStatusService::getRecordFromTaskStatus(const TaskStatusPtr & task) const
{   
    Block block;

    auto string_type = std::make_shared<DB::DataTypeString>();
    auto uint64_type = std::make_shared<DB::DataTypeUInt64>();

    auto make_column_with_type = [](auto & col_type, auto && col_val, auto & name)-> std::shared_ptr<ColumnWithTypeAndName> {
        auto col = col_type->createColumn();
        auto col_inner = typeid_cast<IColumn *>(col.get());
        col_inner->insert(col_val);
        return std::make_shared<ColumnWithTypeAndName>(std::move(col), col_type, name);
    };
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto now_nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(now);

    assert(!task->id.empty());
    String user = task->user.empty() ? "default" : task->user;

    block.insert(*make_column_with_type(string_type, task->id, "id"));
    block.insert(*make_column_with_type(string_type, task->status, "status"));
    block.insert(*make_column_with_type(string_type, task->progress, "progress"));
    block.insert(*make_column_with_type(string_type, task->reason, "reason"));
    block.insert(*make_column_with_type(string_type, task->user, "user"));
    block.insert(*make_column_with_type(string_type, task->context, "context"));
    block.insert(*make_column_with_type(uint64_type, static_cast<uint64_t>(now_nanoseconds.count()), "last_modified"));
    block.insert(*make_column_with_type(uint64_type, static_cast<uint64_t>(now_nanoseconds.count()), "created"));

    return std::make_shared<IDistributedWriteAheadLog::Record>(IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block));
}

TaskStatusService::TaskStatusPtr TaskStatusService::getTaskStatusFromRecord(const IDistributedWriteAheadLog::RecordPtr & record) const
{
    auto task = std::make_shared<TaskStatus>();
    task->id = record->block.getByName("id").column->getDataAt(0).toString();
    task->status = record->block.getByName("status").column->getDataAt(0).toString();
    task->progress = record->block.getByName("progress").column->getDataAt(0).toString();
    task->reason = record->block.getByName("reason").column->getDataAt(0).toString();
    task->user = record->block.getByName("user").column->getDataAt(0).toString();
    task->context = record->block.getByName("context").column->getDataAt(0).toString();
    task->last_modified = record->block.getByName("last_modified").column->get64(0);
    task->created = record->block.getByName("created").column->get64(0);
    task->recorded = std::chrono::steady_clock::now();
    return task;
}


TaskStatusService::TaskStatusPtr TaskStatusService::findById(const String & id)
{
    if(auto task_ptr = findByIdInMemory(id))
    {
        return task_ptr;
    }
    return findByIdInTable(id);
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInMemory(const String & id)
{
    std::shared_lock guard(rwlock);
    auto task = indexedById.find(id);
    if(task == indexedById.end())
    {
        return nullptr;
    }
    return task->second;
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInTable(const String & id)
{
    (void)id;
    assert(0);
    return nullptr;
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::findByUser(const String & user)
{
    auto tasks_in_memory =  findByUserInMemory(user);
    auto tasks_in_table =  findByUserInTable(user);
    auto & bigger_list = tasks_in_memory.size() > tasks_in_table.size() ? tasks_in_memory : tasks_in_table;
    auto & smaller_list = bigger_list == tasks_in_memory ? tasks_in_table : tasks_in_memory;
    bigger_list.insert(bigger_list.end(), smaller_list.begin(), smaller_list.end());
    return bigger_list;
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::findByUserInMemory(const String & user)
{
    std::shared_lock guard(rwlock);
    auto it = indexedByUser.find(user);
    if(it == indexedByUser.end())
    {
        return std::vector<TaskStatusService::TaskStatusPtr>();
    }
    return it->second;
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::findByUserInTable(const String & user)
{
    (void)user;
    assert(0);
    return std::vector<TaskStatusService::TaskStatusPtr>();
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::taskStatuses() const
{
    /// Get all task status (memory, table)
    std::vector<TaskStatusService::TaskStatusPtr> tasks;
    for(auto & it : indexedById)
    {
        tasks.push_back(it.second);
    }
    return tasks;
}

void TaskStatusService::startupService()
{
    createTaskTable();

    auto task_holder = global_context.getSchedulePool().createTask("DumpTask", [this]() { this->dumpFinishedTask(); });
    dump_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*dump_task)->activate();
    (*dump_task)->schedule();
}

void TaskStatusService::shutdownService()
{
    std::unique_lock guard(rwlock);
    (*dump_task)->deactivate();
}

void TaskStatusService::dumpFinishedTask()
{
    std::vector<TaskStatusPtr> finished_tasks;
    { 
        std::unique_lock guard(rwlock);
        for (auto it = indexedById.begin(); it != indexedById.end(); )
        {
            if(it->second->status != TaskStatus::SUCCEEDED && 
               it->second->status != TaskStatus::FAILED)
            {
                it++;
                continue;
            }

            /// Remove task from indexedByUser map
            auto it_map = indexedByUser.find(it->second->user);
            if(it_map != indexedByUser.end())
            {
                auto & user_task = it_map->second;
                for(auto it_user_task = user_task.begin(); it_user_task != user_task.end(); )
                {
                    if((*it_user_task)->id != it->second->id)
                    {
                        it_user_task++;
                        continue;
                    }
                    else
                    {
                        it_user_task = user_task.erase(it_user_task);
                        break;
                    }
                }
            }

            finished_tasks.push_back(std::move(it->second));

            it = indexedById.erase(it);
        }
    }

    for(const auto & task : finished_tasks)
    {
        insertTask(task);
    }
    LOG_INFO(log, "task service dump task every 60s");
    /// Checkpoint

    (*dump_task)->scheduleAfter(reschedule_time_ms);
}

void TaskStatusService::createTaskTable()
{  
    String query = "CREATE TABLE IF NOT EXISTS \
                    default.task \
                    ( \
                    `id` String, \
                    `status` String, \
                    `progress` String, \
                    `reason` String, \
                    `user` String, \
                    `context` String, \
                    `created` DateTime64, \
                    `last_modified` DateTime64 \
                    ) \
                    ENGINE = DistributedMergeTree(1, 1, rand()) \
                    ORDER BY created \
                    SETTINGS index_granularity = 8192";
    Context context = global_context;
    context.makeQueryContext();
    context.setCurrentQueryId("test-task-create-query-id");

    try
    {
        auto stream = executeQuery(query, context, true, QueryProcessingStage::Enum::WithMergeableStateAfterAggregation, false);
        stream.onFinish();
    }
    catch(const DB::Exception & e)
    {
        LOG_ERROR(log, "Create task table failed, ", e.displayText());
    }
    
}

void TaskStatusService::insertTask(const TaskStatusPtr & task)
{
    /// Only SUCCEEDED or FAILED task should be inserted into table
    assert(task->status == TaskStatus::SUCCEEDED || task->status == TaskStatus::FAILED);

    auto & date_lut = DateLUT::instance();

    /// TODO:
    String query_template = "INSERT INTO default.task \
                    (id, status, progress, reason, user, context, created, last_modified) \
                    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')";
    String query = fmt::format(query_template, 
                    task->id,
                    task->status,
                    task->progress,
                    task->reason,
                    task->user,
                    task->context,
                    date_lut.timeToString(task->created),
                    date_lut.timeToString(task->last_modified));

    Context context = global_context;
    context.makeQueryContext();
    context.setCurrentQueryId(task->id);

    CurrentThread::get().attachQueryContext(context);

    ReadBufferFromString in(query);
    String dummy_string;
    WriteBufferFromString out(dummy_string);

    try
    {
        executeQuery(in, out, /* allow_into_outfile = */ false, context, {});
    }
    catch(const DB::Exception & e)
    {
        LOG_ERROR(log, "Insert task failed, ", e.displayText());
    }
}

void TaskStatusService::processQueryWithProcessors(QueryPipeline & pipeline, const std::function<void(Block && )> & callback)
{
    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;

    while (executor.pull(block, 100))
    {
        if (block)
        {
            callback(std::move(block));
            assert(!block);
        }
    }
}

void TaskStatusService::processQuery(BlockInputStreamPtr & in, const std::function<void(Block && )> & callback)
{
    AsynchronousBlockInputStream async_in(in);
    async_in.readPrefix();

    while (true)
    {
        if (async_in.poll(100))
        {
            Block block{async_in.read()};
            if (!block)
            {
                break;
            }

            callback(std::move(block));
            assert(!block);
        }
    }

    async_in.readSuffix();
}

}
