#include "IngestRawStoreHandler.h"
#include "JSONHelper.h"
#include "SchemaValidator.h"

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/executeQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int INVALID_CONFIG_PARAMETER;
}

String IngestRawStoreHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status) const
{
    const auto & database_name = getPathParameter("database", "");
    const auto & table_name = getPathParameter("rawstore", "");

    /// Read enrichment and pass the settings to context
    if (database_name.empty() || table_name.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse("Database or Table is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    String query = "INSERT into " + database_name + "." + table_name + " FORMAT RawStoreEachRow ";

    /// Parse JSON into ReadBuffers
    PODArray<char> parse_buf;
    JSONReadBuffers buffers;
    String error;
    if (!readIntoBuffers(input, parse_buf, buffers, error))
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        LOG_ERROR(
            log,
            "Ingest to database {}, rawstore {} failed with invalid JSON request, exception = {}",
            database_name,
            table_name,
            error,
            ErrorCodes::INCORRECT_DATA);
        return jsonErrorResponse(error, ErrorCodes::INCORRECT_DATA);
    }

    /// Handle "enrichment"
    auto it = buffers.find("enrichment");
    if (it != buffers.end())
    {
        if (!handleEnrichment(*it->second, error))
        {
            http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            LOG_ERROR(
                log,
                "Ingest to database {}, rawstore {} failed with invalid request, exception = {}",
                database_name,
                table_name,
                error,
                ErrorCodes::INCORRECT_DATA);
            return jsonErrorResponse("error", ErrorCodes::INCORRECT_DATA);
        }
    }

    /// Prepare ReadBuffer for executeQuery
    it = buffers.find("data");
    std::unique_ptr<ReadBuffer> in;
    if (it != buffers.end())
    {
        ReadBufferFromString query_buf(query);
        in = std::make_unique<ConcatReadBuffer>(query_buf, *it->second);
    }
    else
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        LOG_ERROR(
            log,
            "Ingest to database {}, rawstore {} failed with invalid request, exception = {}",
            database_name,
            table_name,
            "Invalid Request, missing 'data' field",
            ErrorCodes::INCORRECT_DATA);
        return jsonErrorResponse("Invalid Request, missing 'data' field", ErrorCodes::INCORRECT_DATA);
    }

    if (hasQueryParameter("mode"))
    {
        query_context->setIngestMode(getQueryParameter("mode"));
    }
    else
    {
        query_context->setIngestMode("async");
    }

    String dummy_string;
    WriteBufferFromString out(dummy_string);

    query_context->setSetting("output_format_parallel_formatting", false);
    query_context->setSetting("date_time_input_format", String{"best_effort"});
    executeQuery(*in, out, /* allow_into_outfile = */ false, query_context, {});

    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getClientInfo().current_query_id);
    const auto & poll_id = query_context->getQueryStatusPollId();
    if (!poll_id.empty())
    {
        resp.set("poll_id", poll_id);
        resp.set("channel", query_context->getChannel());
    }
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

bool IngestRawStoreHandler::handleEnrichment(ReadBuffer & buf, String & error) const
{
    const char * begin = buf.internalBuffer().begin();
    const char * end = buf.internalBuffer().end();
    String time_extraction_type;
    String time_extraction_rule;

    SimpleJSON obj{begin, end};

    for (auto it = obj.begin(); it != obj.end(); ++it)
    {
        const auto & name = it.getName();

        if (name == "time_extraction_type" && it.getType() == SimpleJSON::TYPE_NAME_VALUE_PAIR)
            time_extraction_type = it.getValue().getString();
        else if (name == "time_extraction_rule" && it.getType() == SimpleJSON::TYPE_NAME_VALUE_PAIR)
            time_extraction_rule = it.getValue().getString();
    }

    if (!time_extraction_type.empty() && !time_extraction_rule.empty())
    {
        if (time_extraction_type == "json_path" || time_extraction_type == "regex")
        {
            query_context->setSetting("rawstore_time_extraction_type", time_extraction_type);
            query_context->setSetting("rawstore_time_extraction_rule", time_extraction_rule);
            return true;
        }
        else
        {
            error = "Invalid enrichment, only 'json_path' and 'regex' are supported right now";
            return false;
        }
    }
    else if (!time_extraction_type.empty() || !time_extraction_rule.empty())
    {
        error = "Invalid enrichment, either 'rawstore_time_extraction_type' or 'rawstore_time_extraction_rule' is missing ";
        return false;
    }

    return true;
}
}
