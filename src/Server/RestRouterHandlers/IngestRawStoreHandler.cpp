#include "IngestRawStoreHandler.h"
#include "SchemaValidator.h"

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <common/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INCORRECT_DATA;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
    inline void skipToChar(char c, ReadBuffer & buf)
    {
        skipWhitespaceIfAny(buf);
        assertChar(c, buf);
        skipWhitespaceIfAny(buf);
    }
}

std::map<String, std::map<String, String>> IngestRawStoreHandler::enrichment_schema
    = {{"optional", {{"time_extraction_type", "string"}, {"time_extraction_rule", "string"}}}};

String IngestRawStoreHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status) const
{
    String database_name = getPathParameter("database", "");
    String table_name = getPathParameter("rawstore", "");

    /// Read enrichment and pass the settings to context
    if (database_name.empty() || table_name.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse("Database or Table is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    String query = "INSERT into " + database_name + "." + table_name + " FORMAT RawStoreEachRow ";
    ReadBufferFromString query_buf(query);
    std::unique_ptr<ReadBuffer> in = std::make_unique<ConcatReadBuffer>(query_buf, input);

    try
    {
        handleEnrichment(input);
    }
    catch (Exception e)
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse(e.message(), e.code());
    }

    /// No change, to executeQuery
    String dummy_string;
    WriteBufferFromString out(dummy_string);

    query_context.setSetting("output_format_parallel_formatting", false);
    executeQuery(*in, out, /* allow_into_outfile = */ false, query_context, {});

    Poco::JSON::Object resp;
    resp.set("query_id", query_context.getClientInfo().initial_query_id);
    const auto & poll_id = query_context.getQueryStatusPollId();
    if (!poll_id.empty())
    {
        resp.set("poll_id", poll_id);
    }
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

void IngestRawStoreHandler::handleEnrichment(ReadBuffer & buf) const
{
    skipToChar('{', buf);

    /// Read enrichment first
    if (!buf.eof())
    {
        String name;
        readJSONString(name, buf);
        if (name == "enrichment")
        {
            String value = readJSONField(buf);

            if (value.empty())
                return;

            Poco::JSON::Parser parser;
            Poco::JSON::Object::Ptr result;

            try
            {
                auto var = parser.parse(value);
                result = var.extract<Poco::JSON::Object::Ptr>();
            }
            catch (Poco::JSON::JSONException & exception)
            {
                LOG_ERROR(log, exception.message());
                throw Exception("parse 'enrichment' failed, inner exception is: " + exception.message(), ErrorCodes::INCORRECT_DATA);
            }

            String error;
            if (!validateSchema(enrichment_schema, result, error))
                throw Exception(error, ErrorCodes::INCORRECT_DATA);

            if (result->has("time_extraction_type") && result->has("time_extraction_rule"))
            {
                query_context.setSetting("rawstore_time_extraction_type", result->get("time_extraction_type").toString());
                query_context.setSetting("rawstore_time_extraction_rule", result->get("time_extraction_rule").toString());
            }
            else if (result->has("time_extraction_type") || result->has("time_extraction_rule"))
            {
                throw std::make_pair(
                    "Invalid enrichment, either 'rawstore_time_extraction_type' or 'rawstore_time_extraction_rule' is missing ",
                    ErrorCodes::INCORRECT_DATA);
            }
        }
    }

    /// move position to the start of data field
    if (!buf.eof())
    {
        String name;
        readJSONString(name, buf);

        if (name == "data")
        {
            skipToChar(':', buf);
            return;
        }
    }

    throw Exception("Invalid Request", ErrorCodes::INCORRECT_DATA);
}

String IngestRawStoreHandler::readJSONField(ReadBuffer & buf)
{
    String s;
    skipToChar(':', buf);
    bool in_brace = false;
    while (!buf.eof())
    {
        if (!in_brace)
        {
            if (*buf.position() != '{')
            {
                char err[2] = {'{', '\0'};
                throwAtAssertionFailed(err, buf);
            }
            in_brace = true;
        }


        char * next_pos = find_first_symbols<'}'>(buf.position() + 1, buf.buffer().end());

        s.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;

        if (buf.hasPendingData())
        {
            if (*buf.position() == '}')
            {
                ++buf.position();
                s.append(1, '}');
            }
            skipToChar(',', buf);
            return s;
        }
    }
    s.clear();
    return s;
}
}
