#include "IngestStatusHandler.h"

#include <IO/HTTPCommon.h>
#include <Storages/StorageDistributedMergeTree.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int POLL_ID_NOT_EXIST;
    extern const int SEND_POLL_REQ_ERROR;
}

void IngestStatusHandler::prepare()
{
}

String IngestStatusHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    std::pair<String, Int32> progress;

    String poll_id = getPathParameter("poll_id", "");
    if (poll_id.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonException("Empty poll id", ErrorCodes::INVALID_POLL_ID);
    }

    /// components: 0: query_id, 1: database, 2:table,  3: user, 4: node_identity, 5: timestamp
    std::vector<String> components = query_context.parseQueryStatusPollId(poll_id);
    const auto target_node = components[4];
    const auto database_name = components[1];
    const auto table_name = components[2];

    if (target_node == query_context.getNodeIdentity())
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);
        if (!storage)
        {
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE;
            return jsonException("table: " + database_name + "." + table_name + " does not exist", ErrorCodes::UNKNOWN_TABLE);
        }

        if (storage->getName() != "DistributedMergeTree")
        {

            http_status = Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE;
            return jsonException("table: " + database_name + "." + table_name + " is not a DistributedMergeTreeTable", ErrorCodes::TYPE_MISMATCH);
        }

        const auto* distributed = static_cast<const StorageDistributedMergeTree *>(storage.get());
        progress = distributed->getProgress(poll_id);
        if (progress.second < 0)
        {
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
            return jsonException("poll_id does not exists", ErrorCodes::POLL_ID_NOT_EXIST);
        }
        return makeResponse(progress);
    }
    else
    {
        Poco::URI uri{"http://" + target_node + query_context.getConfigRef().getString("http_port") + "/dae/ingest/statuses/" + poll_id};
        String resp = forwardRequest(uri, http_status);
        return resp;
    }
}

String IngestStatusHandler::forwardRequest(const Poco::URI & uri, Int32 & http_status ) const
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

        auto * ostr = &session->sendRequest(request);

        if (!ostr->good())
        {
            http_status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            error = "Failed on uri=" + uri.toString();
            LOG_ERROR(log, error);
            return jsonException(error, ErrorCodes::SEND_POLL_REQ_ERROR);
        }

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
        error = "Failed on uri=" + uri.toString() +  " error=" + e.message() + " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    catch (...)
    {
        error = "Failed on uri=" + uri.toString() +  " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    http_status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    return jsonException(error, ErrorCodes::SEND_POLL_REQ_ERROR);
}

String IngestStatusHandler::makeResponse(const std::pair<String, Int32> & progress)
{
    Poco::JSON::Object resp;
    resp.set("status", progress.first);
    resp.set("progress", progress.second);
    std::stringstream resp_str_stream;  /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}
