#include "DDLService.h"

#include "CatalogService.h"
#include "CommonUtils.h"
#include "PlacementService.h"

#include <Core/Block.h>
#include <IO/HTTPCommon.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <common/logger_useful.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_EXCEPTION;
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
    : global_context(global_context_)
    , catalog(CatalogService::instance(global_context_))
    , placement(PlacementService::instance(global_context_))
    , dwal(DistributedWriteAheadLogPool::instance().getDefault())
    , log(&Poco::Logger::get("DDLService"))
{
    init();
}

DDLService::~DDLService()
{
    shutdown();
}

void DDLService::shutdown()
{
    if (stopped.test_and_set())
    {
        /// already shutdown
        return;
    }

    LOG_INFO(log, "DDLService is stopping");
    if (ddl)
    {
        ddl->wait();
    }
    LOG_INFO(log, "DDLService stopped");

}

bool DDLService::validateSchema(const Block & block, const std::vector<String> & col_names) const
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

std::vector<Poco::URI> DDLService::placeReplicas(Int32 shards, Int32 replication_factor) const
{
    (void)shards;
    (void)replication_factor;
    return {};
}

Int32 DDLService::postRequest(const String & query, const Poco::URI & uri) const
{
    LOG_INFO(log, "Execute table statement={} on uri={}", query, uri.toString());

    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({1, 0}, {1, 0}, {1, 0});

    auto session = makePooledHTTPSession(uri, timeouts, 1);

    try
    {
        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());
        request.setChunkedTransferEncoding(true);

        auto ostr = &session->sendRequest(request);
        ostr->write(query.c_str(), query.size());
        ostr->flush();

        if (!ostr->good())
        {
            LOG_ERROR(log, "Failed to execute table statement={} on uri={}", query, uri.toString());
            return ErrorCodes::UNKNOWN_EXCEPTION;
        }

        Poco::Net::HTTPResponse response;
        /// FIXME: handle table exists, table not exists etc error
        receiveResponse(*session, request, response, false);

        LOG_INFO(log, "Executed table statement={} on uri={} successfully", query, uri.toString());
        return ErrorCodes::OK;
    }
    catch (const Poco::Exception & e)
    {
        session->attachSessionData(e.message());
        LOG_ERROR(
            log,
            "Failed to execute table statement={} on uri={} error={} exception={}",
            query,
            uri.toString(),
            e.message(),
            getCurrentExceptionMessage(true, true));
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to execute table statement={} on uri={} exception={}",
            query,
            uri.toString(),
            getCurrentExceptionMessage(true, true));
    }
    return ErrorCodes::UNKNOWN_EXCEPTION;
}

/// Try indefininitely
Int32 DDLService::doTable(const String & query, const Poco::URI & uri) const
{
    /// FIXME, retry several times and report error
    while (1)
    {
        try
        {
            auto err = postRequest(query, uri);
            if (err == ErrorCodes::OK)
            {
                return err;
            }
        }
        catch (...)
        {
            LOG_ERROR(
                log, "Failed to send request={} to uri={} exception={}", query, uri.toString(), getCurrentExceptionMessage(true, true));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void DDLService::createTable(const Block & block) const
{
    if (!validateSchema(block, {"ddl", "shards", "replication_factor", "timestamp", "query_id"}))
    {
        return;
    }

    /// FIXME : check catalog service if table exists

    Int32 shards = block.getByName("shards").column->getInt(0);
    Int32 replication_factor = block.getByName("replication_factor").column->getInt(0);

    /// 1) Ask placement service to do shard placement
    std::vector<Poco::URI> target_hosts{placeReplicas(shards, replication_factor)};

    assert(target_hosts.size() == static_cast<size_t>(shards * replication_factor));

    String query = block.getByName("ddl").column->getDataAt(0).toString();

    /// 2) Create table on each target host accordign to placement
    for (Int32 i = 0; i < replication_factor; ++i)
    {
        for (Int32 j = 0; j < shards; ++j)
        {
            /// FIXME, check table engine, grammar check
            String create_query = query + ", dwal_partition=" + std::to_string(j);
            doTable(create_query, target_hosts[i * replication_factor + j]);
        }
    }

    /// FIXME: timestamp, query_id
    /// 3) Notify task service, query_id's status
}

void DDLService::mutateTable(const Block & block) const
{
    if (!validateSchema(block, {"ddl", "table", "timestamp", "query_id"}))
    {
        return;
    }

    String table = block.getByName("table").column->getDataAt(0).toString();

    std::vector<Poco::URI> target_hosts{placedReplicas(table)};

    String query = block.getByName("ddl").column->getDataAt(0).toString();
    for (auto & uri : target_hosts)
    {
        doTable(query, uri);
    }
}

void DDLService::processDDL(const IDistributedWriteAheadLog::RecordPtrs & records) const
{
    for (const auto & record : records)
    {
        if (record->op_code == IDistributedWriteAheadLog::OpCode::CREATE_TABLE)
        {
            createTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::DELETE_TABLE)
        {
            mutateTable(record->block);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::ALTER_TABLE)
        {
            mutateTable(record->block);
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
