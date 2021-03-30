#pragma once

#include "MetadataService.h"

#include <Core/BackgroundSchedulePool.h>
#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{

class QueryPipeline;


class TaskStatusService final : public MetadataService
{
public:
    using SteadyClock = std::chrono::time_point<std::chrono::steady_clock>;

    struct TaskStatus
    {
        String id;
        /// Overall general status
        String status;
        /// Application specific progress status
        String progress;
        String reason;
        String user;
        String context;
        UInt64 last_modified;
        UInt64 created;
        /// `recorded` timestamp is for local tracking
        SteadyClock recorded;

        /// Overall general status codes
        static const String SUBMITTED;
        static const String INPROGRESS;
        static const String SUCCEEDED;
        static const String FAILED;
    };

    using TaskStatusPtr = std::shared_ptr<TaskStatus>;

public:
    static TaskStatusService & instance(Context & global_context_);

    explicit TaskStatusService(Context & global_context_);
    virtual ~TaskStatusService() override = default;

    /// Append
    Int32 append(TaskStatusPtr task);

    TaskStatusPtr findById(const String & id);
    std::vector<TaskStatusPtr> findByUser(const String & user);
    std::vector<TaskStatusPtr> taskStatuses() const;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;

    void startupService() override;
    void shutdownService() override;

    String role() const override { return "task"; }
    ConfigSettings configSettings() const override;
    
    TaskStatusPtr findByIdInMemory(const String & id);
    TaskStatusPtr findByIdInTable(const String & id);

    std::vector<TaskStatusPtr> findByUserInMemory(const String & user);
    std::vector<TaskStatusPtr> findByUserInTable(const String & user);

    IDistributedWriteAheadLog::RecordPtr getRecordFromTaskStatus(const TaskStatusPtr & task) const;
    TaskStatusPtr getTaskStatusFromRecord(const IDistributedWriteAheadLog::RecordPtr & record) const;

    void processQuery(BlockInputStreamPtr & in, const std::function<void(Block &&)> & callback = std::function<void(Block &&)>());

    void processQueryWithProcessors(QueryPipeline & pipeline, const std::function<void(Block &&)> & callback = std::function<void(Block &&)>());

    void createTaskTable();
    void insertTask(const TaskStatusPtr & task);
    void dumpFinishedTask();

/// Need to setup a cron job which will check:
/// 1. Task is finish: put them into table
/// 2. Task is hang: warning customer (once or periodically ? what frequency)

private:
    std::shared_mutex rwlock;
    std::unordered_map<String, TaskStatusPtr> indexedById;
    std::unordered_map<String, std::vector<TaskStatusPtr>> indexedByUser;

    std::mutex lock;
    std::deque<std::pair<SteadyClock, String>> timedTasks;
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> dump_task;
    static constexpr size_t reschedule_time_ms = 60000;
};
}
