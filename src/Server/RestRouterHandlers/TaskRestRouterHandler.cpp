#include "TaskRestRouterHandler.h"
#include <common/DateLUT.h>

namespace DB
{

String TaskRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    auto key = getPathParameter("key");
    auto value = getPathParameter("value");
    if(key.empty() || value.empty())
    {
        http_status = 404;
        return "'key' Not Found";
    }
    
    auto & task_service = TaskStatusService::instance(query_context);
    
    Poco::JSON::Array tasks_array;
    if (key == "id")
    {
        std::vector<TaskStatusService::TaskStatusPtr> tasks;
        tasks.push_back(task_service.findById(value));
        tasks_array = getResult(tasks);
    }
    else if (key == "user")
    {
        auto tasks = task_service.findByUser(value);
        tasks_array =  getResult(tasks);
    }
    
    Poco::JSON::Object result;
    result.set("query_id", query_context.getCurrentQueryId());
    result.set("tasks", tasks_array);

    std::stringstream res_string;
    result.stringify(res_string, 4);

    return res_string.str();
}

Poco::JSON::Array TaskRestRouterHandler::getResult(std::vector<TaskStatusService::TaskStatusPtr> & tasks)
{
    Poco::JSON::Array task_list;
    for(auto & task : tasks)
    {
        auto & date_lut = DateLUT::instance();

        Poco::JSON::Object task_object;
        task_object.set("id", task->id);
        task_object.set("status", task->status);
        task_object.set("progress", task->progress);
        task_object.set("reason", task->reason);
        task_object.set("user", task->user);
        task_object.set("context", task->context);
        task_object.set("last_modified", date_lut.timeToString(task->last_modified));
        task_object.set("created", date_lut.timeToString(task->created));

        task_list.add(task_object);
    }
    return task_list;
}

};
