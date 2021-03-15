#include "IngestRestRouterHandler.h"

#include <IO/JSON2QueryReadBuffer.h>

#include <Interpreters/executeQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int POLL_ID_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
}

String IngestRestRouterHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status)
{
    database_name = getPathParameter("database", "");
    table_name = getPathParameter("table", "");

    if(database_name.empty() || table_name.empty())
        throw Exception("Database or Table is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);

    http_status = Poco::Net::HTTPResponse::HTTP_OK;

    std::unique_ptr<ReadBuffer> in = std::make_unique<JSON2QueryReadBuffer>(wrapReadBufferReference(input), database_name + "." + table_name);
    std::shared_ptr<WriteBuffer> used_output = nullptr;

    /// Cannot be set here, since query_id is unknown.
    std::optional<CurrentThread::QueryScope> query_scope;

    query_scope.emplace(query_context);
    executeQuery(*in, *used_output, /* allow_into_outfile = */ false, query_context, {});

    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getClientInfo().initial_query_id);
    resp.set("poll_id", query_context.getQueryStatusPollId());
    std::stringstream  resp_str_stream;
    resp.stringify( resp_str_stream, 4);

    return resp_str_stream.str();
}

}
