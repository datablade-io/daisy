#include "DDLService.h"
#include "CommonUtils.h"

#include <Core/Block.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <common/logger_useful.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern int OK;
}

namespace
{
/// globals
String SYSTEM_ROLES_KEY = "system_settings.system_roles";
String DDL_ROLE = "ddl";

String DDL_KEY_PREFIX = "system_settings.system_ddl_dwal.";
String DDL_NAME_KEY = DDL_KEY_PREFIX + "name";
String DDL_REPLICATION_FACTOR_KEY = DDL_KEY_PREFIX + "replication_factor";
String DDL_DEFAULT_TOPIC = "__system_ddls";
}

DDLService::DDLService(Context & global_context_)
    : global_context(global_context_), dwal(DistributedWriteAheadLogPool::instance().getDefault()), log(&Poco::Logger::get("DDLService"))
{
    init();
}

DDLService::~DDLService()
{
    stopped.test_and_set();

    if (ddl)
    {
        ddl->wait();
    }
}

bool DDLService::validateSchema(const Block & block, const std::vector<String> & col_names)
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

void DDLService::createTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }

    String query = block.getByName("ddl").column->getDataAt(0).toString();
    (void)query;
}

void DDLService::deleteTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }
}

void DDLService::alterTable(const Block & block)
{
    if (!validateSchema(block, {"ddl", "timestamp", "query_id"}))
    {
        return;
    }
}

void DDLService::processDDL(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (const auto & record : records)
    {
        if (record->op_code == IDistributedWriteAheadLog::OpCode::CREATE_TABLE)
        {
            createTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::DELETE_TABLE)
        {
            deleteTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::ALTER_TABLE)
        {
            alterTable(record->block);
        }
        else
        {
            assert(0);
            LOG_ERROR(log, "Unknown operation={}", static_cast<Int32>(record->op_code));
        }
    }
}

void DDLService::backgroundDDL()
{
    createDWal(dwal, ddl_ctx, stopped, log);

    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ddl_ctx);

    /// FIXME, checkpoint
    while (!stopped.test())
    {
        auto result{dwal->consume(10, 200, ddl_ctx)};
        if (result.err != ErrorCodes::OK)
        {
            LOG_ERROR(log, "Failed to consume data, error={}", result.err);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        if (result.records.empty())
        {
            continue;
        }

        processDDL(result.records);
    }
}

void DDLService::init()
{
    const auto & config = global_context.getConfigRef();

    /// if this node has `ddl` role, start background thread doing ddl work
    Poco::Util::AbstractConfiguration::Keys role_keys;
    config.keys(SYSTEM_ROLES_KEY, role_keys);

    for (const auto & key : role_keys)
    {
        if (config.getString(SYSTEM_ROLES_KEY + "." + key, "") == DDL_ROLE)
        {
            LOG_INFO(log, "Detects the current log has `ddl` role");

            /// ddl
            String ddl_topic = config.getString(DDL_NAME_KEY, DDL_DEFAULT_TOPIC);
            DistributedWriteAheadLogKafkaContext ddl_kctx{ddl_topic, 1, config.getInt(DDL_REPLICATION_FACTOR_KEY, 1)};
            ddl_kctx.partition = 0;

            ddl.emplace(1);
            ddl->scheduleOrThrowOnError([this] { backgroundDDL(); });
            ddl_ctx = ddl_kctx;

            break;
        }
    }
}

}
