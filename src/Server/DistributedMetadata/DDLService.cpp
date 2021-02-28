#include "DDLService.h"

#include "CatalogService.h"
#include "PlacementService.h"

#include <Core/Block.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

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
    String DDL_KEY_PREFIX = "system_settings.system_ddl_dwal.";
    String DDL_NAME_KEY = DDL_KEY_PREFIX + "name";
    String DDL_REPLICATION_FACTOR_KEY = DDL_KEY_PREFIX + "replication_factor";
    String DDL_DATA_RETENTION_KEY = DDL_KEY_PREFIX + "data_retention";
    String DDL_DEFAULT_TOPIC = "__system_ddls";
}

DDLService & DDLService::instance(Context & global_context)
{
    static DDLService ddl_service{global_context};
    return ddl_service;
}

DDLService::DDLService(Context & global_context_)
    : MetadataService(global_context_, "DDLService")
    , http_port(":" + global_context_.getConfigRef().getString("http_port"))
    , catalog(CatalogService::instance(global_context_))
    , placement(PlacementService::instance(global_context_))
{
}

MetadataService::ConfigSettings DDLService::configSettings() const
{
    return {
        .name_key = DDL_NAME_KEY,
        .default_name = DDL_DEFAULT_TOPIC,
        .data_retention_key = DDL_DATA_RETENTION_KEY,
        .default_data_retention = 168,
        .replication_factor_key = DDL_REPLICATION_FACTOR_KEY,
        .auto_offset_reset = "earliest",
    };
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

std::vector<Poco::URI> DDLService::place(const std::function<std::vector<String>()> & func) const
{
    std::vector<String> hosts{func()};
    std::vector<Poco::URI> uris;
    uris.reserve(hosts.size());

    for (const auto & host : hosts)
    {
        uris.emplace_back(host + http_port);
    }

    return uris;
}

std::vector<Poco::URI> DDLService::placeReplicas(Int32 shards, Int32 replication_factor) const
{
    auto func = [this, shards, replication_factor]() { return placement.place(shards, replication_factor); };
    return place(func);
}

std::vector<Poco::URI> DDLService::placedReplicas(const String & table) const
{
    auto func = [this, &table]() { return placement.placed(table); };
    return place(func);
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
        /// FIXME: handle table exists, table not exists etc error explicitly
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
    if (!validateSchema(block, {"ddl", "table", "shards", "replication_factor", "timestamp", "query_id"}))
    {
        return;
    }

    /// FIXME : check catalog service if table exists

    Int32 shards = block.getByName("shards").column->getInt(0);
    Int32 replication_factor = block.getByName("replication_factor").column->getInt(0);
    String query = block.getByName("ddl").column->getDataAt(0).toString();

    /// 1) Ask placement service to do shard placement
    std::vector<Poco::URI> target_hosts{placeReplicas(shards, replication_factor)};

    if (target_hosts.empty())
    {
        String table = block.getByName("table").column->getDataAt(0).toString();
        LOG_ERROR(
            log,
            "Failed to create table={} because there are not enough hosts to place its total={} shard replicas, statement={}",
            table,
            shards * replication_factor, query);
        /// FIXME, project task status
        return;
    }

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
    /// 3) Project task status for query_id
}

void DDLService::mutateTable(const Block & block) const
{
    if (!validateSchema(block, {"ddl", "table", "timestamp", "query_id"}))
    {
        return;
    }

    String table = block.getByName("table").column->getDataAt(0).toString();

    std::vector<Poco::URI> target_hosts{placedReplicas(table)};

    if (target_hosts.empty())
    {
        /// Fail this mutation coz table is not found
        /// FIXME: make sure `target_hosts` is a complete list of hosts which
        /// has this table definition (shards * replication_factor)
        LOG_ERROR(log, "Table {} is not found", table);

        /// FIXME, project task status
        return;
    }

    String query = block.getByName("ddl").column->getDataAt(0).toString();
    for (auto & uri : target_hosts)
    {
        doTable(query, uri);
    }

    /// FIXME, project task status
}

void DDLService::commit(Int64 last_sn)
{
    /// FIXME, retries
    try
    {
        auto err = dwal->commit(last_sn, dwal_ctx);
        if (likely(err == 0))
        {
            LOG_INFO(log, "Successfully committed offset={}", last_sn);
        }
        else
        {
            /// It is ok as next commit will override this commit if it makes through.
            /// If it failed and then crashes, we will redo and we will find resource
            /// already exists or resource not exists errors which shall be handled in
            /// DDL processing functions. In this case, for idempotent DDL like create
            /// table or delete table, it shall be OK. For alter table, it may depend ?
            LOG_ERROR(log, "Failed to commit offset={} error={}", last_sn, err);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit offset={} exception={}",
            last_sn,
            getCurrentExceptionMessage(true, true));
    }
}

void DDLService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
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

    const_cast<DDLService *>(this)->commit(records.back()->sn);
}
}
