#include "IngestStatusHandler.h"

#include <IO/HTTPCommon.h>
#include <Storages/StorageDistributedMergeTree.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <boost/algorithm/string/split.hpp>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int POLL_ID_DOESNT_EXIST;
    extern const int SEND_POLL_REQ_ERROR;
}

void IngestStatusHandler::prepare()
{

}

String IngestStatusHandler::executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    http_status = Poco::Net::HTTPResponse::HTTP_OK;
    Int32 progress;

    String poll_id = getPathParameter("poll_id", "");
    if (poll_id.empty())
    {
        throw Exception("Empty poll id", ErrorCodes::INVALID_POLL_ID);
    }

    std::vector<String> components = query_context.parseQueryStatusPollId(poll_id);
    String target_node = components[3];
    std::vector<String> names;
    String sep = ".";
    boost::algorithm::split(names, components[1], boost::is_any_of(sep));
    if (names.size() !=2)
    {
        throw Exception("Invalid poll_id" + poll_id, ErrorCodes::INVALID_POLL_ID);
    }
    String database_name = names[0];
    String table_name = names[1];

    if(payload)
        LOG_INFO(log, "Impossible for GET Method, payload is nullptr");

    if(target_node == query_context.getNodeIdentity())
    {
        StoragePtr ptr = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);
        if(!ptr)
        {
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE;
            throw Exception("table: " + database_name + "." + table_name +" does not exist", ErrorCodes::UNKNOWN_TABLE);
        }

        if (const auto * storage = dynamic_cast<const StorageDistributedMergeTree *>(ptr.get()))
        {
            progress = storage->getProgress(poll_id);
            LOG_INFO(log, "Progress is : {}", progress);
            if(progress < 0)
            {
                http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
                throw Exception("poll_id does not exists", ErrorCodes::POLL_ID_DOESNT_EXIST);
            }
            return makeResponse(progress);
        } else {
            /// it is not a DistributedMergeTreeTable, throw exception
            http_status = Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE;
            throw Exception("table: " + database_name + "." + table_name + " is not a DistributedMergeTreeTable", ErrorCodes::TYPE_MISMATCH);
        }
    } else {
        Poco::URI uri{"http://" + target_node + ":8123/dae/ingest/statuses/" + poll_id};
        String resp = sendGetRequest(uri);
        LOG_INFO(log, "Progress is : " + resp);
        return resp;
    }
}

String IngestStatusHandler::sendGetRequest(const Poco::URI & uri) const
{
    LOG_INFO(log, "Send GET request to on uri={}", uri.toString());

    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({1, 0}, {1, 0}, {1, 0});

    PooledHTTPSessionPtr session;
    try
    {
        session = makePooledHTTPSession(uri, timeouts, 1);
        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());

        auto *ostr = &session->sendRequest(request);

        if (!ostr->good())
        {
            LOG_ERROR(log, "Failed on uri={}", uri.toString());
            throw Exception( "Failed to send request to url: " + uri.toString(), ErrorCodes::SEND_POLL_REQ_ERROR);
        }

        Poco::Net::HTTPResponse response;
        auto & istr = session->receiveResponse(response);
        auto status = response.getStatus();
        if (status != Poco::Net::HTTPResponse::HTTP_OK)
        {
            LOG_INFO(log, "Executed on uri={} failed", uri.toString());
        }
        return std::string(std::istreambuf_iterator<char>(istr), {});
    }
    catch (const Poco::Exception & e)
    {
        if (!session.isNull())
        {
            session->attachSessionData(e.message());
        }

        LOG_ERROR(
            log,
            "Failed on uri={} error={} exception={}",
            uri.toString(),
            e.message(),
            getCurrentExceptionMessage(true, true));

        throw Exception("Failed on uri: " + uri.toString() + " error=" + e.message(), ErrorCodes::SEND_POLL_REQ_ERROR);
    }
    catch (...)
    {
        String msg = getCurrentExceptionMessage(true, true);
        LOG_ERROR(
            log,
            "Failed to execute on uri={} exception={}",
            uri.toString(),
            msg);

        throw Exception("Failed on uri: " + uri.toString() + " error=" + msg, ErrorCodes::SEND_POLL_REQ_ERROR);
    }
}

String IngestStatusHandler::makeResponse(const Int32 progress)
{
    Poco::JSON::Object resp;
    resp.set("progress", progress);
    std::stringstream  resp_str_stream;
    resp.stringify( resp_str_stream, 4);
    return resp_str_stream.str();
}
}
