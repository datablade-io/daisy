#pragma once

#include "MetadataService.h"

namespace DB
{
class TaskService final : public MetadataService
{
public:
    using SteadyClock = std::chrono::time_point<std::chrono::steady_clock>;

    struct Task
    {
        String id;
        String status;
        String progress;
        String user;
        UInt64 last_modified;
        UInt64 created;
        /// `recorded` timestamp is for local tracking
        SteadyClock recorded;
    };

    using TaskPtr = std::shared_ptr<Task>;

public:
    static TaskService & instance(Context & global_context_);

    explicit TaskService(Context & global_context_);
    virtual ~TaskService() = default;

    TaskPtr findById(const String & id) const;
    std::vector<TaskPtr> findByUser(const String & user) const;
    std::vector<TaskPtr> tasks() const;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "task"; }
    ConfigSettings configSettings() const override;

private:
    std::shared_mutex rwlock;
    std::unordered_map<String, TaskPtr> indexedById;
    std::unordered_map<String, std::vector<TaskPtr>> indexedByUser;

    std::mutex lock;
    std::deque<std::pair<SteadyClock, String>> timedTasks;
};
}
