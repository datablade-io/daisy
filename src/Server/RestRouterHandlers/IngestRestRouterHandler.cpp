#include "IngestRestRouterHandler.h"

#include <IO/JSON2QueryReadBuffer.h>
#include <IO/WriteBufferFromString.h>

#include <Interpreters/executeQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

String IngestRestRouterHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status) const
{
    String database_name = getPathParameter("database", "");
    String table_name = getPathParameter("table", "");

    if (database_name.empty() || table_name.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse("Database or Table is empty", ErrorCodes::BAD_REQUEST_PARAMETER);
    }

    query_context.setSetting("output_format_parallel_formatting", false);

    std::unique_ptr<ReadBuffer> in
        = std::make_unique<JSON2QueryReadBuffer>(wrapReadBufferReference(input), database_name + "." + table_name);
    String dummy_string;
    WriteBufferFromString out(dummy_string);

    executeQuery(*in, out, /* allow_into_outfile = */ false, query_context, {});

    /// Send back ingest response
    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getCurrentQueryId());
    const auto & poll_id = query_context.getQueryStatusPollId();
    if (!poll_id.empty())
    {
        resp.set("poll_id", poll_id);
    }
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

}
