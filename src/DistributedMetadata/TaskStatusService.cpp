#include "TaskStatusService.h"

#include <Core/Block.h>
#include <DataStreams/BlockIO.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/BlockUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeSelectQuery.h>

#include <common/ClockUtils.h>
#include <common/DateLUT.h>
#include <common/logger_useful.h>

#include <Poco/Util/Application.h>

#include <chrono>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

namespace DB
{
namespace
{
String TASK_KEY_PREFIX = "system_settings.system_task_dwal.";
String TASK_NAME_KEY = TASK_KEY_PREFIX + "name";
String TASK_REPLICATION_FACTOR_KEY = TASK_KEY_PREFIX + "replication_factor";
String TASK_DATA_RETENTION_KEY = TASK_KEY_PREFIX + "data_retention";
String TASK_DEFAULT_TOPIC = "__system_tasks";

Block buildBlock(const std::vector<TaskStatusService::TaskStatusPtr> & tasks)
{
    std::vector<std::pair<String, std::vector<String>>> string_cols
        = {{"id", std::vector<String>()},
            {"status", std::vector<String>()},
            {"progress", std::vector<String>()},
            {"reason", std::vector<String>()},
            {"user", std::vector<String>()},
            {"context", std::vector<String>()}};
    std::vector<std::pair<String, std::vector<Int64>>> int64_cols
        = {{"last_modified", std::vector<Int64>()}, {"created", std::vector<Int64>()}, {"recorded", std::vector<Int64>()}};

    for (const auto & task : tasks)
    {
        for (auto & col : string_cols)
        {
            if ("id" == col.first)
            {
                col.second.push_back(task->id);
            }
            else if ("status" == col.first)
            {
                col.second.push_back(task->status);
            }
            else if ("progress" == col.first)
            {
                col.second.push_back(task->progress);
            }
            else if ("reason" == col.first)
            {
                col.second.push_back(task->reason);
            }
            else if ("user" == col.first)
            {
                col.second.push_back(task->user);
            }
            else if ("context" == col.first)
            {
                col.second.push_back(task->context);
            }
            else
            {
                assert(false);
            }
        }
        for (auto & col : int64_cols)
        {
            if ("last_modified" == col.first)
            {
                col.second.push_back(task->last_modified == -1 ? (UTCMilliseconds::now()) : task->last_modified);
            }
            else if ("created" == col.first)
            {
                col.second.push_back(task->created == -1 ? (UTCMilliseconds::now()) : task->created);
            }
            else if ("recorded" == col.first)
            {
                col.second.push_back(MonotonicMilliseconds::now());
            }
            else
            {
                assert(false);
            }
        }
    }

    return DB::buildBlock(string_cols, int64_cols);
}

IDistributedWriteAheadLog::Record buildRecord(const TaskStatusService::TaskStatusPtr & task)
{
    std::vector<TaskStatusService::TaskStatusPtr> tasks = {task};
    auto block = buildBlock(tasks);
    return IDistributedWriteAheadLog::Record(IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK, std::move(block));
}
}

const String TaskStatusService::TaskStatus::SUBMITTED = "SUBMITTED";
const String TaskStatusService::TaskStatus::INPROGRESS = "INPROGRESS";
const String TaskStatusService::TaskStatus::SUCCEEDED = "SUCCEEDED";
const String TaskStatusService::TaskStatus::FAILED = "FAILED";


TaskStatusService & TaskStatusService::instance(const ContextPtr & global_context_)
{
    static TaskStatusService task_status_service{global_context_};
    return task_status_service;
}

TaskStatusService::TaskStatusService(const ContextPtr & global_context_) : MetadataService(global_context_, "TaskStatusService")
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
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
    };
}

Int32 TaskStatusService::append(TaskStatusPtr task)
{
    auto record = buildRecord(task);
    dwal->append(record, dwal_append_ctx);
    return 0;
}

void TaskStatusService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    /// Consume records and build in-memory indexes
    for (const auto & record : records)
    {
        assert(record->op_code == IDistributedWriteAheadLog::OpCode::ADD_DATA_BLOCK);

        auto task_ptr = buildTaskStatusFromRecord(record);
        updateTaskStatus(task_ptr);
    }
}

void TaskStatusService::updateTaskStatus(TaskStatusPtr & task_ptr)
{
    std::unique_lock guard(indexes_lock);

    auto id_map_iter = indexed_by_id.find(task_ptr->id);
    if (id_map_iter == indexed_by_id.end())
    {
        indexed_by_id[task_ptr->id] = task_ptr;
        auto user_map_iter = indexed_by_user.find(task_ptr->user);
        if(user_map_iter == indexed_by_user.end())
        {
            indexed_by_user[task_ptr->user] = std::unordered_map<String, TaskStatusPtr>();
            user_map_iter = indexed_by_user.find(task_ptr->user);
        }

        assert(user_map_iter != indexed_by_user.end());
        assert(user_map_iter->second.find(task_ptr->id) == user_map_iter->second.end());

        user_map_iter->second[task_ptr->id] = task_ptr;
        return;
    }

    assert(id_map_iter->second->id == task_ptr->id);

    /// Do not copy created field to an exist task
    id_map_iter->second->status = task_ptr->status;
    id_map_iter->second->progress = task_ptr->progress;
    id_map_iter->second->reason = task_ptr->reason;
    id_map_iter->second->user = task_ptr->user;
    id_map_iter->second->context = task_ptr->context;
    id_map_iter->second->last_modified = task_ptr->last_modified;

    /// Assert pointer in id_map and pointer in user_map are always point to the same object
    auto user_map_iter = indexed_by_user.find(task_ptr->user);
    assert(user_map_iter != indexed_by_user.end());

    auto task_in_user_map_iter = user_map_iter->second.find(task_ptr->id);
    assert(task_in_user_map_iter != user_map_iter->second.end());
    assert(id_map_iter->second.get() == task_in_user_map_iter->second.get());
}

bool TaskStatusService::validateSchema(const Block & block, const std::vector<String> & col_names) const
{
    for (const auto & col_name : col_names)
    {
        if (!block.has(col_name))
        {
            LOG_ERROR(log, "`{}` column is missing", col_name);
            return false;
        }
    }
    return true;
}

bool TaskStatusService::tableExists() const
{
    bool exists = false;
    ContextPtr query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    executeSelectQuery(
        "SELECT count(*) AS c FROM system.tables WHERE database = 'system' AND name = 'tasks'", query_context, [&exists](Block && block) {
            assert(block.has("c"));
            const auto & counts_col = block.findByName("c")->column;
            exists = counts_col->getUInt(0) != 0;
        });
    return exists;
}

TaskStatusService::TaskStatusPtr TaskStatusService::buildTaskStatusFromRecord(const IDistributedWriteAheadLog::RecordPtr & record) const
{
    std::vector<TaskStatusService::TaskStatusPtr> tasks;
    buildTaskStatusFromBlock(record->block, tasks);
    assert(tasks.size() > 0);
    return tasks[0];
}

void TaskStatusService::buildTaskStatusFromBlock(const Block & block, std::vector<TaskStatusService::TaskStatusPtr> & res) const
{
    validateSchema(block, {"id", "status", "progress", "reason", "user", "context", "last_modified", "created"});

    const auto & id_col = block.findByName("id")->column;
    const auto & status_col = block.findByName("status")->column;
    const auto & progress_col = block.findByName("progress")->column;
    const auto & reason_col = block.findByName("reason")->column;
    const auto & user_col = block.findByName("user")->column;
    const auto & context_col = block.findByName("context")->column;
    const auto & last_modified_col = block.findByName("last_modified")->column;
    const auto & created_col = block.findByName("created")->column;
    const auto recorded_col_with_type_and_name = block.findByName("recorded");
    const auto & recorded_col = recorded_col_with_type_and_name ? recorded_col_with_type_and_name->column : nullptr;

    for (size_t i = 0; i < id_col->size(); ++i)
    {
        auto task = std::make_shared<TaskStatus>();
        task->id = id_col->getDataAt(i).toString();
        task->status = status_col->getDataAt(i).toString();
        task->progress = progress_col->getDataAt(i).toString();
        task->reason = reason_col->getDataAt(i).toString();
        task->user = user_col->getDataAt(i).toString();
        task->context = context_col->getDataAt(i).toString();
        task->last_modified = last_modified_col->getInt(i);
        task->created = created_col->getInt(i);
        if (recorded_col)
        {
            task->recorded = std::chrono::steady_clock::time_point(std::chrono::milliseconds(recorded_col->get64(i)));
        }

        assert(task->last_modified);
        assert(task->created);

        res.push_back(task);
    }
}

TaskStatusService::TaskStatusPtr TaskStatusService::findById(const String & id)
{
    if (auto task_ptr = findByIdInMemory(id))
    {
        return task_ptr;
    }
    return findByIdInTable(id);
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInMemory(const String & id)
{
    std::shared_lock guard(indexes_lock);
    if (auto task = indexed_by_id.find(id); task != indexed_by_id.end())
    {
        return task->second;
    }

    for (auto task_list_iter = finished_tasks.begin(); task_list_iter != finished_tasks.end(); ++task_list_iter)
    {
        for(const auto & task : task_list_iter->second)
        {
            if(task->id == id)
            {
                return task;
            }
        }
    }
    return nullptr;
}

TaskStatusService::TaskStatusPtr TaskStatusService::findByIdInTable(const String & id)
{
    auto query_template = "SELECT DISTINCT id, status, progress, "
                          "reason, user, context, created, last_modified "
                          "FROM system.tasks "
                          "WHERE user <> '' AND id == '{}' "
                          "ORDER BY last_modified DESC";
    auto query = fmt::format(query_template, id);

    std::vector<TaskStatusService::TaskStatusPtr> res;

    ContextPtr query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();

    executeSelectQuery(query, query_context, [this, &res](Block && block) { this->buildTaskStatusFromBlock(block, res); });
    if (res.size() == 0)
        return nullptr;
    return res[0];
}

std::vector<TaskStatusService::TaskStatusPtr> TaskStatusService::findByUser(const String & user)
{
    std::vector<TaskStatusService::TaskStatusPtr> res;
    findByUserInMemory(user, res);
    findByUserInTable(user, res);

    return res;
}

void TaskStatusService::findByUserInMemory(const String & user, std::vector<TaskStatusService::TaskStatusPtr> & res)
{
    std::shared_lock guard(indexes_lock);
    auto user_map_iter = indexed_by_user.find(user);
    if (user_map_iter == indexed_by_user.end())
    {
        return;
    }

    for(auto it = user_map_iter->second.begin(); it != user_map_iter->second.end(); ++it)
    {
        res.push_back(it->second);
    }

    for (auto it = finished_tasks.begin(); it != finished_tasks.end(); ++it)
    {
        for(const auto & task : it->second)
        {
            if (task->user == user)
            {
                res.push_back(task);
            }
        }
    }
}

void TaskStatusService::findByUserInTable(const String & user, std::vector<TaskStatusService::TaskStatusPtr> & res)
{
    assert(!user.empty());
    auto query_template = "SELECT DISTINCT id, status, progress, "
                          "reason, user, context, created, last_modified "
                          "FROM system.tasks "
                          "WHERE user == '{}' "
                          "ORDER BY last_modified DESC";
    auto query = fmt::format(query_template, user);

    ContextPtr query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();

    executeSelectQuery(query, query_context, [this, &res](Block && block) { this->buildTaskStatusFromBlock(block, res); });
}

void TaskStatusService::schedulePersistentTask()
{
    for(int i = 0; i < RETRY_TIMES; ++i)
    {
        if(createTaskTable())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(RETRY_INTERVAL_MS));
    }

    auto task_holder = global_context->getSchedulePool().createTask("PersistentTask", [this]() { this->persistentFinishedTask(); });
    persistent_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*persistent_task)->activate();
    (*persistent_task)->schedule();
}

void TaskStatusService::preShutdown()
{
    if (persistent_task)
        (*persistent_task)->deactivate();
}

void TaskStatusService::persistentFinishedTask()
{
    {
        std::unique_lock guard(indexes_lock);
        for (auto it = indexed_by_id.begin(); it != indexed_by_id.end();)
        {
            if (it->second->status != TaskStatus::SUCCEEDED && it->second->status != TaskStatus::FAILED)
            {
                it++;
                continue;
            }

            /// Remove task from indexed_by_user map
            auto user_map_iter = indexed_by_user.find(it->second->user);
            assert(user_map_iter != indexed_by_user.end());

            auto task_iter = user_map_iter->second.find(it->second->id);
            assert(task_iter != user_map_iter->second.end());
            
            user_map_iter->second.erase(task_iter);
            assert(user_map_iter->second.find(it->second->id) == user_map_iter->second.end());

            if(user_map_iter->second.empty())
            {
                indexed_by_user.erase(user_map_iter);
                assert(indexed_by_user.find(it->second->user) == indexed_by_user.end());
            }

            auto finished_task_iter = finished_tasks.find(it->second->last_modified);
            if(finished_task_iter == finished_tasks.end())
            {
                finished_tasks[it->second->last_modified] = std::vector<TaskStatusPtr>{it->second};
            }
            else
            {
                finished_task_iter->second.push_back(it->second);
            }

            it = indexed_by_id.erase(it);
        }
    }

    std::vector<TaskStatusPtr> persistent_list;

    for (auto finished_task_iter = finished_tasks.begin(); finished_task_iter != finished_tasks.end();)
    {
        if (MonotonicSeconds::now() - finished_task_iter->first < CACHE_FINISHED_TASK_MS)
        {
            ++finished_task_iter;
            continue;
        }
        persistent_list.insert(persistent_list.end(), finished_task_iter->second.begin(), finished_task_iter->second.end());

        finished_task_iter = finished_tasks.erase(finished_task_iter);
    }

    if (!persistent_list.empty())
    {
        LOG_DEBUG(log, "Persistent {} finished tasks", persistent_list.size());
    }

    (*persistent_task)->scheduleAfter(RESCHEDULE_TIME_MS);
}

bool TaskStatusService::createTaskTable()
{
    const auto & config = global_context->getConfigRef();
    const auto & conf = configSettings();
    const auto replicas = std::to_string(config.getInt(conf.replication_factor_key, 1));

    String query = fmt::format("CREATE TABLE IF NOT EXISTS \
                    system.tasks \
                    ( \
                    `id` String, \
                    `status` String, \
                    `progress` String, \
                    `reason` String, \
                    `user` String, \
                    `context` String, \
                    `created` DateTime64(3), \
                    `last_modified` DateTime64(3), \
                    `_time` DateTime64(3) DEFAULT created) \
                    ENGINE = DistributedMergeTree(1,{},rand()) \
                    ORDER BY (toMinute(_time), user, id) \
                    PARTITION BY toDate(_time) \
                    TTL toDateTime(_time + toIntervalDay(7)) DELETE \
                    SETTINGS index_granularity = 8192", replicas);

    /// FIXME: Remove query_payload when the sql interface is implemented
    String query_payload = R"d(\{
        "name" : "tasks",
        "shards": 1,
        "replication_factor": )d" + replicas + R"d(,
        "shard_by_expression": "rand()",
        "columns" : [
        {
        "name" : "id",
        "type" : "String"
        },
        {
        "name" : "status",
        "type" : "String"
        },
        {
        "name" : "progress",
        "type" : "String"
        },
        {
        "name" : "reason",
        "type" : "String"
        },
        {
        "name" : "user",
        "type" : "String"
        },
        {
        "name" : "context",
        "type" : "String"
        },
        {
        "name" : "created",
        "type" : "DateTime64(3)"
        },
        {
        "name" : "last_modified",
        "type" : "DateTime64(3)"
        }],
        "order_by_expression" : "(toMinute(_time), user, id)",
        "partition_by_granularity" : "D",
        "order_by_granularity": "H",
        "ttl_expression" : "toDateTime(_time + toIntervalDay(7)) DELETE",
        "_time_column": "created"
        }
    )d";

    ContextPtr context = Context::createCopy(global_context);
    context->setCurrentQueryId("");
    context->setQueryParameter("_payload", query_payload);
    context->setDistributedDDLOperation(true);
    CurrentThread::QueryScope query_scope{context};
    

    try
    {
        if (tableExists())
        {
            return true;
        }

        auto stream = executeQuery(query, context, true, QueryProcessingStage::Enum::WithMergeableStateAfterAggregation, false);
        stream.onFinish();
    }
    catch (...)
    {
        LOG_ERROR(log, "Create task table failed. ", getCurrentExceptionMessage(true, true));
        return false;
    }
    return true;
}

bool TaskStatusService::persistentTaskStatuses(const std::vector<TaskStatusPtr> & tasks)
{
    assert(!tasks.empty());
    String query = "INSERT INTO system.tasks \
                    (id, status, progress, reason, user, context, created, last_modified) \
                    VALUES ";

    ContextPtr context = Context::createCopy(global_context);
    context->setCurrentQueryId("");
    CurrentThread::QueryScope query_scope{context};

    const String value_template = "{}('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')";
    String delimiter = "";
    auto & date_lut = DateLUT::instance();

    for (const auto & task : tasks)
    {
        /// Only SUCCEEDED or FAILED task should be persistented into table
        assert(task->status == TaskStatus::SUCCEEDED || task->status == TaskStatus::FAILED);
        auto created = date_lut.timeToString(task->created / 1000);
        auto last_modified = date_lut.timeToString(task->last_modified / 1000);
        String value = fmt::format(
            value_template,
            delimiter,
            task->id,
            task->status,
            task->progress,
            task->reason,
            task->user,
            task->context,
            created,
            last_modified);
        query += value;
        delimiter = ",";
    }

    LOG_TRACE(log, "Persistent task value #{}#", query);

    LOG_INFO(log, "Persistent {} tasks. ", tasks.size());
    ReadBufferFromString in(query);
    String dummy_string;
    WriteBufferFromString out(dummy_string);

    try
    {
        executeQuery(in, out, /* allow_into_outfile = */ false, context, {});
    }
    catch (...)
    {
        return false;
        LOG_ERROR(log, "Persistent task failed. ", getCurrentExceptionMessage(true, true));
    }
    return true;
}

}
