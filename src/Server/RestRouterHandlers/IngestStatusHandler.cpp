#include "IngestStatusHandler.h"

#include <DistributedMetadata/PlacementService.h>
#include <IO/HTTPCommon.h>
#include <Storages/StorageDistributedMergeTree.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>

#include <numeric>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int POLL_ID_NOT_EXIST;
    extern const int SEND_POLL_REQ_ERROR;
}

const String BATCH_URL = "http://{}:{}/dae/v1/ingest/statuses";
const String POLL_URL = "http://{}:{}/dae/v1/ingest/statuses/{}";

String IngestStatusHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    const String & poll_id = getPathParameter("poll_id", "");
    if (poll_id.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse("Empty poll id", ErrorCodes::INVALID_POLL_ID);
    }

    /// components: 0: query_id, 1: database, 2: table, 3: user, 4: node_identity, 5: timestamp
    std::vector<String> components = query_context->parseQueryStatusPollId(poll_id);
    const auto & target_node = components[4];
    const auto & database_name = components[1];
    const auto & table_name = components[2];

    if (target_node == query_context->getNodeIdentity())
    {
        String error;
        int error_code = ErrorCodes::OK;
        const StorageDistributedMergeTree * storage = getAndVerifyStorage(database_name, table_name, error, error_code);
        if (!storage)
        {
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE;
            return jsonErrorResponse(error, error_code);
        }

        auto status = storage->getIngestStatus(poll_id);
        if (status.second < 0)
        {
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
            return jsonErrorResponse("poll_id does not exists", ErrorCodes::POLL_ID_NOT_EXIST);
        }
        return makeResponse(status);
    }
    else
    {
        Poco::URI uri{fmt::format(POLL_URL, target_node, query_context->getConfigRef().getString("http_port"), poll_id)};
        return forwardRequest(uri, nullptr, http_status);
    }
}

bool IngestStatusHandler::parsePollIds(const std::vector<String> & poll_ids, TableQueries & queries, String & error) const
{
    error.clear();

    for (const auto & poll_id : poll_ids)
    {
        std::vector<String> components;
        try
        {
            /// components: 0: query_id, 1: database, 2: table, 3: user, 5: timestamp
            components = query_context->parseQueryStatusPollId(poll_id);
        }
        catch (Exception & e)
        {
            error = "Invalid query id: " + poll_id + " ErrorCode: " + std::to_string(e.code());
            return false;
        }
        const auto & full_name = components[1] + "." + components[2];
        auto & ids = queries[full_name];
        ids.emplace_back(poll_id);
    }

    return true;
}

String IngestStatusHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String error;
    PlacementService & placement = PlacementService::instance(query_context);
    const String & channel_id = payload->get("channel_id").toString();
    const String & target_node = placement.getNodeIdentityByChannelId(channel_id);

    if (target_node.empty())
    {
        /// Invalid node
        http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
        return jsonErrorResponse("Invalid channel_id", ErrorCodes::POLL_ID_NOT_EXIST);
    }

    if (target_node == query_context->getNodeIdentity())
    {
        const auto & arr = payload->getArray("poll_ids");
        std::vector<String> poll_ids;
        for (const auto & poll_id : *arr)
            poll_ids.emplace_back(poll_id.extract<String>());
        TableQueries queries;
        if (!parsePollIds(poll_ids, queries, error))
        {
            http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            return jsonErrorResponse(error, ErrorCodes::INVALID_POLL_ID);
        }

        std::vector<std::tuple<String, String, Int32>> statuses;
        int error_code = ErrorCodes::OK;

        for (auto & query : queries)
        {
            std::vector<String> names;
            String sep = ".";
            boost::algorithm::split(names, query.first, boost::is_any_of(sep));

            const StorageDistributedMergeTree * storage = getAndVerifyStorage(names[0], names[1], error, error_code);
            if (!storage)
            {
                LOG_ERROR(
                    log,
                    "{}, for poll_ids: {}",
                    error,
                    std::accumulate(query.second.begin(), query.second.end(), std::string{","}),
                    error_code);
                continue;
            }
            storage->getStatusInBatch(query.second, statuses);
        }
        if (statuses.empty())
        {
            http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            return jsonErrorResponse("None of poll_id in 'poll_ids' is valid", ErrorCodes::INVALID_POLL_ID);
        }
        return makeBatchResponse(statuses);
    }
    else
    {
        Poco::URI uri{fmt::format(BATCH_URL, target_node, query_context->getConfigRef().getString("http_port"))};
        return forwardRequest(uri, payload, http_status);
    }
}

String IngestStatusHandler::forwardRequest(const Poco::URI & uri, const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    LOG_DEBUG(log, "Send GET request to on uri={}", uri.toString());

    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({1, 0}, {1, 0}, {5, 0});

    String error;
    PooledHTTPSessionPtr session;
    try
    {
        session = makePooledHTTPSession(uri, timeouts, 1);
        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());

        std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        if (payload)
        {
            payload->stringify(req_body_stream, 0);
            request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
            const String & body = req_body_stream.str();
            request.setContentType("application/json");
            request.setContentLength(body.length());
        }
        std::ostream & ostr = session->sendRequest(request);
        if (!ostr.good())
        {
            http_status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            error = "Failed on uri=" + uri.toString();
            LOG_ERROR(log, error);
            return jsonErrorResponse(error, ErrorCodes::SEND_POLL_REQ_ERROR);
        }

        if (payload)
            ostr << req_body_stream.str();

        Poco::Net::HTTPResponse response;
        auto & istr = session->receiveResponse(response);
        http_status = response.getStatus();

        if (http_status != Poco::Net::HTTPResponse::HTTP_OK)
        {
            LOG_INFO(log, "Executed on uri={} failed", uri.toString());
        }
        return String(std::istreambuf_iterator<char>(istr), {});
    }
    catch (const Poco::Exception & e)
    {
        if (!session.isNull())
        {
            session->attachSessionData(e.message());
        }
        error = "Failed on uri=" + uri.toString() + " error=" + e.message() + " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    catch (...)
    {
        error = "Failed on uri=" + uri.toString() + " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    http_status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    return jsonErrorResponse(error, ErrorCodes::SEND_POLL_REQ_ERROR);
}

const StorageDistributedMergeTree *
IngestStatusHandler::getAndVerifyStorage(const String & database_name, const String & table_name, String & error, int & error_code) const
{
    error.clear();
    error_code = ErrorCodes::OK;

    StoragePtr storage;
    try
    {
        storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);
    }
    catch (Exception & e)
    {
        error = e.message();
        error_code = e.code();
        return nullptr;
    }

    if (!storage)
    {
        error = "table: " + database_name + "." + table_name + " does not exist";
        error_code = ErrorCodes::UNKNOWN_TABLE;
        return nullptr;
    }

    if (storage->getName() != "DistributedMergeTree")
    {
        error = "table: " + database_name + "." + table_name + " is not a DistributedMergeTreeTable";
        error_code = ErrorCodes::TYPE_MISMATCH;
        return nullptr;
    }
    return static_cast<const StorageDistributedMergeTree *>(storage.get());
}

String IngestStatusHandler::makeResponse(const std::pair<String, Int32> & status)
{
    Poco::JSON::Object resp;
    resp.set("status", status.first);
    resp.set("progress", status.second);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String IngestStatusHandler::makeBatchResponse(const std::vector<std::tuple<String, String, Int32>> & statuses)
{
    Poco::JSON::Object resp;
    Poco::JSON::Array json_statuses;
    for (const auto & status : statuses)
    {
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        json->set("poll_id", std::get<0>(status));
        json->set("status", std::get<1>(status));
        json->set("progress", std::get<2>(status));
        json_statuses.add(json);
    }
    resp.set("status", json_statuses);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}
