#pragma once

#include "MetadataService.h"

namespace DB
{
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
    static TaskStatusService & instance(const ContextPtr & global_context_);

    explicit TaskStatusService(const ContextPtr & global_context_);
    virtual ~TaskStatusService() override = default;

    /// Append
    Int32 append(TaskStatusPtr task);

    TaskStatusPtr findById(const String & id) const;
    std::vector<TaskStatusPtr> findByUser(const String & user) const;
    std::vector<TaskStatusPtr> taskStatuses() const;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "task"; }
    ConfigSettings configSettings() const override;

private:
    std::shared_mutex rwlock;
    std::unordered_map<String, TaskStatusPtr> indexedById;
    std::unordered_map<String, std::vector<TaskStatusPtr>> indexedByUser;

    std::mutex lock;
    std::deque<std::pair<SteadyClock, String>> timedTasks;
};
}
