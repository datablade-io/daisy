#include "IngestRestRouterHandler.h"

#include <IO/JSON2QueryReadBuffer.h>

#include <Interpreters/executeQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

String IngestRestRouterHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status)
{
    String database_name = getPathParameter("database", "");
    String table_name = getPathParameter("table", "");
    String error;

    if(database_name.empty() || table_name.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        error = "Database or Table is empty";
        return jsonException(error, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    std::unique_ptr<ReadBuffer> in = std::make_unique<JSON2QueryReadBuffer>(wrapReadBufferReference(input), database_name + "." + table_name);
    std::shared_ptr<WriteBuffer> used_output = nullptr;

    std::optional<CurrentThread::QueryScope> query_scope;

    query_scope.emplace(query_context);
    executeQuery(*in, *used_output, /* allow_into_outfile = */ false, query_context, {});

    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getClientInfo().initial_query_id);
    const auto & poll_id = query_context.getQueryStatusPollId();
    if (!poll_id.empty())
    {
        resp.set("poll_id", poll_id);
    }
    std::stringstream  resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify( resp_str_stream, 0);

    return resp_str_stream.str();
}

}
