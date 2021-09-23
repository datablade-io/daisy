#include "TableStatsRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>
#include <DistributedMetadata/sendRequest.h>
#include <Storages/DistributedMergeTree/StorageDistributedMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int BAD_REQUEST_PARAMETER;
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{
    const String TABLE_STATS_URL = "http://{}:{}/dae/v1/tablestats/{}?local={}";

    String buildResponse(const String & query_id, Poco::JSON::Object & resp)
    {
        resp.set("request_id", query_id);
        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);

        return resp_str_stream.str();
    }
}

std::pair<String, Int32> TableStatsRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const auto & table = getPathParameter("table");
    if (table.empty())
        return {jsonErrorResponse("Table is empty.", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    /// check table validity
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & table_ptrs = catalog_service.findTableByName(database, table);
    if (table_ptrs.empty())
        return {
            jsonErrorResponse(fmt::format("TABLE '{}.{}' does not exist.", database, table), ErrorCodes::UNKNOWN_TABLE),
            HTTPResponse::HTTP_BAD_REQUEST};

    assert(table_ptrs[0]);
    if (table_ptrs[0]->engine != "DistributedMergeTree")
        return {
            jsonErrorResponse(
                fmt::format("No support TABLE '{}.{}' Engine {}.", database, table, table_ptrs[0]->engine), ErrorCodes::TYPE_MISMATCH),
            HTTPResponse::HTTP_BAD_REQUEST};

    try
    {
        /// if local == true, only local table stats
        if (getQueryParameterBool("local", false))
        {
            Poco::JSON::Object resp;
            int err = buildLocalTableStatsJSON(resp, table);
            if (err != ErrorCodes::OK)
            {
                return {buildResponse(query_context->getCurrentQueryId(), resp), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
            }
            return {buildResponse(query_context->getCurrentQueryId(), resp), HTTPResponse::HTTP_OK};
        }

        /// Forward all nodes where the table is shard/replica to get local table stats.
        TableResponses responses;
        /// TODO: parallel
        for (auto table_ptr : table_ptrs)
        {
            auto node = catalog_service.nodeByIdentity(table_ptr->node_identity);
            Poco::URI uri{fmt::format(TABLE_STATS_URL, node->host, node->http_port, table, /* local=true */ "true")};
            auto [response, http_status] = sendRequest(
                uri,
                Poco::Net::HTTPRequest::HTTP_GET,
                query_context->getCurrentQueryId(),
                query_context->getUserName(),
                query_context->getPasswordByUserName(query_context->getUserName()),
                "",
                log);
            responses.emplace_back(table_ptr->host, table_ptr->shard, response, http_status);
        }
        return {mergeLocalTableStatsToString(responses), HTTPResponse::HTTP_OK};
    }
    catch (std::exception & e)
    {
        return {
            jsonErrorResponse(
                fmt::format("Failed to get table '{}.{}' stats: {}", database, table, e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_BAD_REQUEST};
    }
}

/* * Format Internal Local Table JSON:
* <name>
* <shard>
* <streaming>
*   - topic
*   - partition
*   - app_offset
*   - committed_offset
*   - end_offset
*   - group_id
* <local>
*   - total_rows
*   - toal_bytes
*   - current_sn
*   - startup_max_committed_sn
*   - current_max_committed_sn
*   - parts
*       + active_count
*       + inactive_count
* */
int TableStatsRestRouterHandler::buildLocalTableStatsJSON(Poco::JSON::Object & resp, const String & table) const
{
    const auto & storage = DatabaseCatalog::instance().tryGetTable(StorageID(database, table), query_context);
    if (!storage)
    {
        resp.set("error_msg", fmt::format("TABLE '{}.{}' storage does not exist.", database, table));
        resp.set("code", ErrorCodes::UNKNOWN_TABLE);
        return ErrorCodes::UNKNOWN_TABLE;
    }

    if (storage->isRemote())
    {
        resp.set("error_msg", fmt::format("The {} TABLE '{}.{}' is remote table on local node.", storage->getName(), database, table));
        resp.set("code", ErrorCodes::TYPE_MISMATCH);
        return ErrorCodes::TYPE_MISMATCH;
    }

    auto dstorage = storage->as<StorageDistributedMergeTree>();
    if (!dstorage)
    {
        resp.set("error_msg", fmt::format("No support TABLE '{}.{}' Engine {}.", database, table, storage->getName()));
        resp.set("code", ErrorCodes::TYPE_MISMATCH);
        return ErrorCodes::TYPE_MISMATCH;
    }

    Poco::JSON::Object resp_data;
    /* set name */
    resp_data.set("name", table);

    /* set shard */
    resp_data.set("shard", dstorage->currentShard());

    /* set streaming */
    Poco::JSON::Object streaming_obj;
    auto topic_partition_stats = dstorage->getTopicPartitionStats();
    streaming_obj.set("topic", topic_partition_stats->topic);
    streaming_obj.set("partition", topic_partition_stats->partition);
    streaming_obj.set("app_offset", topic_partition_stats->app_offset);
    streaming_obj.set("committed_offset", topic_partition_stats->committed_offset);
    streaming_obj.set("end_offset", topic_partition_stats->end_offset);
    streaming_obj.set("group_id", topic_partition_stats->group_id);
    resp_data.set("streaming", streaming_obj);

    /* set local */
    Poco::JSON::Object local_obj;
    local_obj.set("total_rows", *(dstorage->totalRows(query_context->getSettingsRef())));
    local_obj.set("total_bytes", *(dstorage->totalBytes(query_context->getSettingsRef())));
    local_obj.set("current_sn", dstorage->lastSN());
    auto [startup_max_committed_sn, current_max_committed_sn] = dstorage->maxPartsCommittedSN();
    local_obj.set("startup_max_committed_sn", startup_max_committed_sn);
    local_obj.set("current_max_committed_sn", current_max_committed_sn);

    Poco::JSON::Object sub_parts_obj;
    sub_parts_obj.set("active_count", dstorage->getDataParts({MergeTreeData::DataPartState::Committed}).size());
    sub_parts_obj.set(
        "inactive_count",
        dstorage
            ->getDataParts(
                {MergeTreeData::DataPartState::Temporary,
                 MergeTreeData::DataPartState::PreCommitted,
                 MergeTreeData::DataPartState::Outdated,
                 MergeTreeData::DataPartState::Deleting,
                 MergeTreeData::DataPartState::DeleteOnDestroy})
            .size());
    local_obj.set("parts", sub_parts_obj);
    resp_data.set("local", local_obj);


    resp.set("data", resp_data);
    return ErrorCodes::OK;
}

String TableStatsRestRouterHandler::mergeLocalTableStatsToString(const TableResponses & table_responses) const
{
    /*** NOTE: Dict <ds>: when [] elem is not exist, then create it. ***/
    using Dict = Poco::DynamicStruct;
    using Var = Poco::Dynamic::Var;
    Var ds{Dict()};
    ds["request_id"] = query_context->getCurrentQueryId();

    /// [FUNC] get or create nest dict ref
    auto dict = [](Var & root, auto... nest_keys) -> Var & {
        Var * curr = &root;
        for (const auto & key : std::vector<String>{nest_keys...})
        {
            auto & tmp = (*curr)[key];
            curr = tmp.isStruct() ? &tmp : &(tmp = Dict());
        }
        return *curr;
    };

    auto & data_dict = dict(ds, "data");
    /// For requests and responses is sequential, we assume that the last stats can be used as lastest value.
    for (const auto & table_resp : table_responses)
    {
        const auto [host, shard, response, status] = table_resp;

        /* local.shards.shard */
        auto & shard_dict = dict(data_dict, "local", "shards", std::to_string(shard));
        shard_dict["shard"] = shard;

        /* local.shards.shard.replicas.host */
        auto & node_dict = dict(shard_dict, "replicas", host);

        /// if one of responses is invaild, we only mark error and continue to others.
        if (status != HTTPResponse::HTTP_OK)
        {
            node_dict = jsonErrorResponseFrom(response);
            continue;
        }

        /*** NOTE: const Dict <response_ds>: when [] elem is not exist, then throw NotFoundException. ***/
        Poco::JSON::Parser parser;
        const Dict & response_ds = *parser.parse(response).extract<Poco::JSON::Object::Ptr>();

        node_dict = response_ds["data"]["local"];

        const auto & resp_streaming = response_ds["data"]["streaming"];
        /* streaming.topic */
        dict(data_dict, "streaming", "topic") = resp_streaming["topic"];

        /* streaming.partitions.partition */
        auto & partition_dict = dict(data_dict, "streaming", "partitions", resp_streaming["partition"].convert<String>());
        partition_dict["partition"] = resp_streaming["partition"];
        partition_dict["end_offset"] = resp_streaming["end_offset"];

        /* streaming.partitions.partition.consumer_groups.consumer_group */
        auto & cgrp_dict = dict(partition_dict, "consumer_groups", resp_streaming["group_id"].extract<String>());
        cgrp_dict["group_id"] = resp_streaming["group_id"];
        cgrp_dict["offset"] = resp_streaming["app_offset"];
        cgrp_dict["committed_offset"] = resp_streaming["committed_offset"];

        /* name */
        data_dict["name"] = response_ds["data"]["name"];
    }

    return ds.toString();
}

}
