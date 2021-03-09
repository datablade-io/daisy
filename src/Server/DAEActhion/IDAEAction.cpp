#include "IDAEAction.h"

#include <chrono>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ExternalTable.h>
#include <DataStreams/IBlockInputStream.h>
#include <Disks/StoragePolicy.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryParameterVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/IServer.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/getFQDNOrHostName.h>
#include <ext/scope_guard.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#include <Poco/File.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>

#include <chrono>
#include <iomanip>

#include <Poco/JSON/Parser.h>

namespace DB
{

void IDAEAction::pushDelayedResults(Output & used_output)
{
    std::vector<WriteBufferPtr> write_buffers;
    std::vector<ReadBufferPtr> read_buffers;
    std::vector<ReadBuffer *> read_buffers_raw_ptr;

    auto * cascade_buffer = typeid_cast<CascadeWriteBuffer *>(used_output.out_maybe_delayed_and_compressed.get());
    if (!cascade_buffer)
        throw Exception("Expected CascadeWriteBuffer", ErrorCodes::LOGICAL_ERROR);

    cascade_buffer->getResultBuffers(write_buffers);

    if (write_buffers.empty())
        throw Exception("At least one buffer is expected to overwrite result into HTTP response", ErrorCodes::LOGICAL_ERROR);

    for (auto & write_buf : write_buffers)
    {
        IReadableWriteBuffer * write_buf_concrete;
        ReadBufferPtr reread_buf;

        if (write_buf
            && (write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
            && (reread_buf = write_buf_concrete->tryGetReadBuffer()))
        {
            read_buffers.emplace_back(reread_buf);
            read_buffers_raw_ptr.emplace_back(reread_buf.get());
        }
    }

    ConcatReadBuffer concat_read_buffer(read_buffers_raw_ptr);
    copyData(concat_read_buffer, *used_output.out_maybe_compressed);
}


static std::chrono::steady_clock::duration parseSessionTimeout(
    const Poco::Util::AbstractConfiguration & config,
    const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

        if (session_timeout > max_session_timeout)
            throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + toString(max_session_timeout)
                            + ". Maximum session timeout could be modified in configuration file.",
                            ErrorCodes::INVALID_SESSION_TIMEOUT);
    }

    return std::chrono::seconds(session_timeout);
}

void IDAEAction::executeByQuery(
        IServer & server,
        Poco::Logger * log,
        Context & context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        Output & used_output,
        std::optional<CurrentThread::QueryScope> & query_scope,
        String & query)
{
// //
// //   LOG_TRACE(log, "Request URI: {}", request.getURI());

//     std::istream & istr = request.stream();
// //    std::string code = "SELECT 1 FORMAT Pretty";
// //    std::stringstream ss{code};
// //    std::istream & istr = ss;

//     /// The user and password can be passed by headers (similar to X-Auth-*),
//     /// which is used by load balancers to pass authentication information.
//     std::string user = request.get("X-ClickHouse-User", "");
//     std::string password = request.get("X-ClickHouse-Key", "");
//     std::string quota_key = request.get("X-ClickHouse-Quota", "");

//     if (user.empty() && password.empty() && quota_key.empty())
//     {
//         /// User name and password can be passed using query parameters
//         /// or using HTTP Basic auth (both methods are insecure).
//         if (request.hasCredentials())
//         {
//             Poco::Net::HTTPBasicCredentials credentials(request);

//             user = credentials.getUsername();
//             password = credentials.getPassword();
//         }
//         else
//         {
//             user = params.get("user", "default");
//             password = params.get("password", "");
//         }

//         quota_key = params.get("quota_key", "");
//     }
//     else
//     {
//         /// It is prohibited to mix different authorization schemes.
//         if (request.hasCredentials()
//             || params.has("user")
//             || params.has("password")
//             || params.has("quota_key"))
//         {
//             throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::REQUIRED_PASSWORD);
//         }
//     }

//     /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

//     ClientInfo & client_info = context.getClientInfo();
//     client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
//     client_info.interface = ClientInfo::Interface::HTTP;

//     ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
//     if (request.getMethod() == PHTTPServerRequest::HTTP_GET)
//         http_method = ClientInfo::HTTPMethod::GET;
//     else if (request.getMethod() == PHTTPServerRequest::HTTP_POST)
//         http_method = ClientInfo::HTTPMethod::POST;

//     client_info.http_method = http_method;
//     client_info.http_user_agent = request.get("User-Agent", "");
//     client_info.http_referer = request.get("Referer", "");
//     client_info.forwarded_for = request.get("X-Forwarded-For", "");

//     /// This will also set client_info.current_user and current_address
//     context.setUser(user, password, request.clientAddress());
//     if (!quota_key.empty())
//         context.setQuotaKey(quota_key);

//     /// Query sent through HTTP interface is initial.
//     client_info.initial_user = client_info.current_user;
//     client_info.initial_address = client_info.current_address;

//     /// The user could specify session identifier and session timeout.
//     /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

//     std::shared_ptr<NamedSession> session;
//     String session_id;
// //    std::chrono::steady_clock::duration session_timeout;
// //    bool session_is_set = params.has("session_id");
// //    const auto & config = server.config();
// //
// //    if (session_is_set)
// //    {
// //        session_id = params.get("session_id");
// //        session_timeout = parseSessionTimeout(config, params);
// //        std::string session_check = params.get("session_check", "");
// //
// //        session = context.acquireNamedSession(session_id, session_timeout, session_check == "1");
// //
// //        context = session->context;
// //        context.setSessionContext(session->context);
// //    }

//     SCOPE_EXIT({
//                    if (session)
//                        session->release();
//                });

//     // Parse the OpenTelemetry traceparent header.
//     // Disable in Arcadia -- it interferes with the
//     // test_clickhouse.TestTracing.test_tracing_via_http_proxy[traceparent] test.
// #if !defined(ARCADIA_BUILD)
//     if (request.has("traceparent"))
//     {
//         std::string opentelemetry_traceparent = request.get("traceparent");
//         std::string error;
//         if (!context.getClientInfo().client_trace_context.parseTraceparentHeader(
//                 opentelemetry_traceparent, error))
//         {
//             throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER,
//                             "Failed to parse OpenTelemetry traceparent header '{}': {}",
//                             opentelemetry_traceparent, error);
//         }

//         context.getClientInfo().client_trace_context.tracestate = request.get("tracestate", "");
//     }
// #endif

//     // Set the query id supplied by the user, if any, and also update the
//     // OpenTelemetry fields.
//     context.setCurrentQueryId(params.get("query_id",
//                                          request.get("X-ClickHouse-Query-Id", "")));

//     client_info.initial_query_id = client_info.current_query_id;

//     /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
//     String http_response_compression_methods = request.get("Accept-Encoding", "");
//     CompressionMethod http_response_compression_method = CompressionMethod::None;

//     if (!http_response_compression_methods.empty())
//     {
//         /// If client supports brotli - it's preferred.
//         /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
//         /// NOTE parsing of the list of methods is slightly incorrect.

//         if (std::string::npos != http_response_compression_methods.find("br"))
//             http_response_compression_method = CompressionMethod::Brotli;
//         else if (std::string::npos != http_response_compression_methods.find("gzip"))
//             http_response_compression_method = CompressionMethod::Gzip;
//         else if (std::string::npos != http_response_compression_methods.find("deflate"))
//             http_response_compression_method = CompressionMethod::Zlib;
//         else if (std::string::npos != http_response_compression_methods.find("xz"))
//             http_response_compression_method = CompressionMethod::Xz;
//         else if (std::string::npos != http_response_compression_methods.find("zstd"))
//             http_response_compression_method = CompressionMethod::Zstd;
//     }

//     bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

//     /// Client can pass a 'compress' flag in the query string. In this case the query result is
//     /// compressed using internal algorithm. This is not reflected in HTTP headers.
//     bool internal_compression = params.getParsed<bool>("compress", false);

//     /// At least, we should postpone sending of first buffer_size result bytes
//     size_t buffer_size_total = std::max(
//             params.getParsed<size_t>("buffer_size", DBMS_DEFAULT_BUFFER_SIZE), static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));

//     /// If it is specified, the whole result will be buffered.
//     ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
//     bool buffer_until_eof = params.getParsed<bool>("wait_end_of_query", false);

//     size_t buffer_size_http = DBMS_DEFAULT_BUFFER_SIZE;
//     size_t buffer_size_memory = (buffer_size_total > buffer_size_http) ? buffer_size_total : 0;

//     unsigned keep_alive_timeout = 10;//config.getUInt("keep_alive_timeout", 10);

//     used_output.out = std::make_shared<WriteBufferFromHTTPServerResponse>(
//             request, response, keep_alive_timeout, client_supports_http_compression, http_response_compression_method);

//     if (internal_compression)
//         used_output.out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*used_output.out);
//     else
//         used_output.out_maybe_compressed = used_output.out;

//     if (buffer_size_memory > 0 || buffer_until_eof)
//     {
//         CascadeWriteBuffer::WriteBufferPtrs cascade_buffer1;
//         CascadeWriteBuffer::WriteBufferConstructors cascade_buffer2;

//         if (buffer_size_memory > 0)
//             cascade_buffer1.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_memory));

//         if (buffer_until_eof)
//         {
//             const std::string tmp_path(context.getTemporaryVolume()->getDisk()->getPath());
//             const std::string tmp_path_template(tmp_path + "http_buffers/");

//             auto create_tmp_disk_buffer = [tmp_path_template] (const WriteBufferPtr &)
//             {
//                 return WriteBufferFromTemporaryFile::create(tmp_path_template);
//             };

//             cascade_buffer2.emplace_back(std::move(create_tmp_disk_buffer));
//         }
//         else
//         {
//             auto push_memory_buffer_and_continue = [next_buffer = used_output.out_maybe_compressed] (const WriteBufferPtr & prev_buf)
//             {
//                 auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
//                 if (!prev_memory_buffer)
//                     throw Exception("Expected MemoryWriteBuffer", ErrorCodes::LOGICAL_ERROR);

//                 auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
//                 copyData(*rdbuf , *next_buffer);

//                 return next_buffer;
//             };

//             cascade_buffer2.emplace_back(push_memory_buffer_and_continue);
//         }

//         used_output.out_maybe_delayed_and_compressed = std::make_shared<CascadeWriteBuffer>(
//                 std::move(cascade_buffer1), std::move(cascade_buffer2));
//     }
//     else
//     {
//         used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
//     }

//     /// Request body can be compressed using algorithm specified in the Content-Encoding header.
//     String http_request_compression_method_str = request.get("Content-Encoding", "");
//     std::unique_ptr<ReadBuffer> in_post = wrapReadBufferWithCompressionMethod(
//             std::make_unique<ReadBufferFromIStream>(istr), chooseCompressionMethod({}, http_request_compression_method_str));

//     /// The data can also be compressed using incompatible internal algorithm. This is indicated by
//     /// 'decompress' query parameter.
//     std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
//     bool in_post_compressed = false;
//     if (params.getParsed<bool>("decompress", false))
//     {
//         in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post);
//         in_post_compressed = true;
//     }
//     else
//         in_post_maybe_compressed = std::move(in_post);

//     std::unique_ptr<ReadBuffer> in;

//     static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
//                                               "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check"};

//     Names reserved_param_suffixes;

//     auto param_could_be_skipped = [&] (const String & name)
//     {
//         /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
//         if (name.empty())
//             return true;

//         if (reserved_param_names.count(name))
//             return true;

//         for (const String & suffix : reserved_param_suffixes)
//         {
//             if (endsWith(name, suffix))
//                 return true;
//         }

//         return false;
//     };

//     /// Settings can be overridden in the query.
//     /// Some parameters (database, default_format, everything used in the code above) do not
//     /// belong to the Settings class.

//     /// 'readonly' setting values mean:
//     /// readonly = 0 - any query is allowed, client can change any setting.
//     /// readonly = 1 - only readonly queries are allowed, client can't change settings.
//     /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

//     /// In theory if initially readonly = 0, the client can change any setting and then set readonly
//     /// to some other value.
//     const auto & settings = context.getSettingsRef();

//     /// Only readonly queries are allowed for HTTP GET requests.
//     if (request.getMethod() == PHTTPServerRequest::HTTP_GET)
//     {
//         if (settings.readonly == 0)
//             context.setSetting("readonly", 2);
//     }

//     bool has_external_data = startsWith(request.getContentType(), "multipart/form-data");

//     if (has_external_data)
//     {
//         /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
//         reserved_param_suffixes.reserve(3);
//         /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
//         reserved_param_suffixes.emplace_back("_format");
//         reserved_param_suffixes.emplace_back("_types");
//         reserved_param_suffixes.emplace_back("_structure");
//     }

//     std::string database = request.get("X-ClickHouse-Database", "");
//     std::string default_format = request.get("X-ClickHouse-Format", "");

//     SettingsChanges settings_changes;
//     for (const auto & [key, value] : params)
//     {
//         if (key == "database")
//         {
//             if (database.empty())
//                 database = value;
//         }
//         else if (key == "default_format")
//         {
//             if (default_format.empty())
//                 default_format = value;
//         }
//         else if (param_could_be_skipped(key))
//         {
//         }
//         else
//         {
//             /// Other than query parameters are treated as settings.
//             // if (!customizeQueryParam(context, key, value))
//             //     settings_changes.push_back({key, value});
//         }
//     }

//     if (!database.empty())
//         context.setCurrentDatabase(database);

//     if (!default_format.empty())
//         context.setDefaultFormat(default_format);

//     /// For external data we also want settings
//     context.checkSettingsConstraints(settings_changes);
//     context.applySettingsChanges(settings_changes);

//     // const auto & query = getQuery(request, params, context);
//     std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);
//     in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

//     /// HTTP response compression is turned on only if the client signalled that they support it
//     /// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
//     used_output.out->setCompression(client_supports_http_compression && settings.enable_http_compression);
//     if (client_supports_http_compression)
//         used_output.out->setCompressionLevel(settings.http_zlib_compression_level);

//     used_output.out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

//     /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
//     /// checksums of client data compressed with internal algorithm are not checked.
//     if (in_post_compressed && settings.http_native_compression_disable_checksumming_on_decompress)
//         static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

//     /// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed
//     /// Origin header.
//     used_output.out->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());

//     auto append_callback = [&context] (ProgressCallback callback)
//     {
//         auto prev = context.getProgressCallback();

//         context.setProgressCallback([prev, callback] (const Progress & progress)
//                                     {
//                                         if (prev)
//                                             prev(progress);

//                                         callback(progress);
//                                     });
//     };

//     /// These 2 progress mode options are mutually exclusive, and
//     /// send_progress_in_http_body overrides send_progress_in_http_headers
//     SendProgressMode send_progress_mode = SendProgressMode::progress_none;
//     if (settings.send_progress_in_http_headers)
//     {
//         send_progress_mode = SendProgressMode::progress_via_header;
//     }

//     if (settings.send_progress_in_http_body)
//     {
//         send_progress_mode = SendProgressMode::progress_via_body;
//     }
//     used_output.out->setSendProgressMode(send_progress_mode);

//     /// While still no data has been sent, we will report about query execution progress
//     /// by sending HTTP body or headers.
//     if (send_progress_mode != SendProgressMode::progress_none)
//         append_callback([&used_output] (const Progress & progress) { used_output.out->onProgress(progress); });

//     if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
//     {
//         Poco::Net::StreamSocket & socket = dynamic_cast<PHTTPServerRequestImpl &>(request).socket();

//         append_callback([&context, &socket](const Progress &)
//                         {
//                             /// Assume that at the point this method is called no one is reading data from the socket any more.
//                             /// True for read-only queries.
//                             try
//                             {
//                                 char b;
//                                 int status = socket.receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK);
//                                 if (status == 0)
//                                     context.killCurrentQuery();
//                             }
//                             catch (Poco::TimeoutException &)
//                             {
//                             }
//                             catch (...)
//                             {
//                                 context.killCurrentQuery();
//                             }
//                         });
//     }

//    // customizeContext(request, context);

//     query_scope.emplace(context);

//     executeQuery(*in, *used_output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
//                  [&response] (const String & current_query_id, const String & content_type, const String & format, const String & timezone)
//                  {
//                      response.setContentType(content_type);
//                      response.add("X-ClickHouse-Query-Id", current_query_id);
//                      response.add("X-ClickHouse-Format", format);
//                      response.add("X-ClickHouse-Timezone", timezone);
//                  }
//     );

//     if (used_output.hasDelayed())
//     {
//         /// TODO: set Content-Length if possible
//         pushDelayedResults(used_output);
//     }

//     /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
//     /// the client.

//     Poco::JSON::Object resp;
//     resp.set("query_id", client_info.initial_query_id);
//     std::stringstream  resp_str_stream;
//     resp.stringify( resp_str_stream, 4);
//     std::string resp_str = resp_str_stream.str();

//     writeString(resp_str, *used_output.out_maybe_delayed_and_compressed);
//     writeChar('\n', *used_output.out_maybe_delayed_and_compressed);
//     used_output.out_maybe_delayed_and_compressed->next();
//     used_output.out->next();

//     used_output.out->finalize();


    LOG_TRACE(log, "Request URI: {}", request.getURI());

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            Poco::Net::HTTPBasicCredentials credentials(request);

            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials()
            || params.has("user")
            || params.has("password")
            || params.has("quota_key"))
        {
            throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::REQUIRED_PASSWORD);
        }
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

    ClientInfo & client_info = context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::HTTP;

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");

    /// This will also set client_info.current_user and current_address
    context.setUser(user, password, request.clientAddress());
    if (!quota_key.empty())
        context.setQuotaKey(quota_key);

    /// Query sent through HTTP interface is initial.
    client_info.initial_user = client_info.current_user;
    client_info.initial_address = client_info.current_address;

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

    std::shared_ptr<NamedSession> session;
    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    if (session_is_set)
    {
        session_id = params.get("session_id");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");

        session = context.acquireNamedSession(session_id, session_timeout, session_check == "1");

        context = session->context;
        context.setSessionContext(session->context);
    }

    SCOPE_EXIT({
        if (session)
            session->release();
    });

    // Parse the OpenTelemetry traceparent header.
    // Disable in Arcadia -- it interferes with the
    // test_clickhouse.TestTracing.test_tracing_via_http_proxy[traceparent] test.
#if !defined(ARCADIA_BUILD)
    if (request.has("traceparent"))
    {
        std::string opentelemetry_traceparent = request.get("traceparent");
        std::string error;
        if (!context.getClientInfo().client_trace_context.parseTraceparentHeader(
            opentelemetry_traceparent, error))
        {
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER,
                "Failed to parse OpenTelemetry traceparent header '{}': {}",
                opentelemetry_traceparent, error);
        }

        context.getClientInfo().client_trace_context.tracestate = request.get("tracestate", "");
    }
#endif

    // Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context.setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));

    client_info.initial_query_id = client_info.current_query_id;

    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
    {
        /// If client supports brotli - it's preferred.
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.

        if (std::string::npos != http_response_compression_methods.find("br"))
            http_response_compression_method = CompressionMethod::Brotli;
        else if (std::string::npos != http_response_compression_methods.find("gzip"))
            http_response_compression_method = CompressionMethod::Gzip;
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
            http_response_compression_method = CompressionMethod::Zlib;
        else if (std::string::npos != http_response_compression_methods.find("xz"))
            http_response_compression_method = CompressionMethod::Xz;
        else if (std::string::npos != http_response_compression_methods.find("zstd"))
            http_response_compression_method = CompressionMethod::Zstd;
    }

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    bool internal_compression = params.getParsed<bool>("compress", false);

    /// At least, we should postpone sending of first buffer_size result bytes
    size_t buffer_size_total = std::max(
        params.getParsed<size_t>("buffer_size", DBMS_DEFAULT_BUFFER_SIZE), static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));

    /// If it is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    bool buffer_until_eof = params.getParsed<bool>("wait_end_of_query", false);

    size_t buffer_size_http = DBMS_DEFAULT_BUFFER_SIZE;
    size_t buffer_size_memory = (buffer_size_total > buffer_size_http) ? buffer_size_total : 0;

    unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

    used_output.out = std::make_shared<WriteBufferFromHTTPServerResponse>(
        response,
        request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD,
        keep_alive_timeout,
        client_supports_http_compression,
        http_response_compression_method);

    if (internal_compression)
        used_output.out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*used_output.out);
    else
        used_output.out_maybe_compressed = used_output.out;

    if (buffer_size_memory > 0 || buffer_until_eof)
    {
        CascadeWriteBuffer::WriteBufferPtrs cascade_buffer1;
        CascadeWriteBuffer::WriteBufferConstructors cascade_buffer2;

        if (buffer_size_memory > 0)
            cascade_buffer1.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_memory));

        if (buffer_until_eof)
        {
            const std::string tmp_path(context.getTemporaryVolume()->getDisk()->getPath());
            const std::string tmp_path_template(tmp_path + "http_buffers/");

            auto create_tmp_disk_buffer = [tmp_path_template] (const WriteBufferPtr &)
            {
                return WriteBufferFromTemporaryFile::create(tmp_path_template);
            };

            cascade_buffer2.emplace_back(std::move(create_tmp_disk_buffer));
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = used_output.out_maybe_compressed] (const WriteBufferPtr & prev_buf)
            {
                auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception("Expected MemoryWriteBuffer", ErrorCodes::LOGICAL_ERROR);

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf , *next_buffer);

                return next_buffer;
            };

            cascade_buffer2.emplace_back(push_memory_buffer_and_continue);
        }

        used_output.out_maybe_delayed_and_compressed = std::make_shared<CascadeWriteBuffer>(
            std::move(cascade_buffer1), std::move(cascade_buffer2));
    }
    else
    {
        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
    }

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    auto in_post = wrapReadBufferWithCompressionMethod(
        wrapReadBufferReference(request.getStream()), chooseCompressionMethod({}, http_request_compression_method_str));

    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    bool in_post_compressed = false;
    if (params.getParsed<bool>("decompress", false))
    {
        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post);
        in_post_compressed = true;
    }
    else
        in_post_maybe_compressed = std::move(in_post);

    std::unique_ptr<ReadBuffer> in;

    static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
        "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check"};

    Names reserved_param_suffixes;

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        if (reserved_param_names.count(name))
            return true;

        for (const String & suffix : reserved_param_suffixes)
        {
            if (endsWith(name, suffix))
                return true;
        }

        return false;
    };

    /// Settings can be overridden in the query.
    /// Some parameters (database, default_format, everything used in the code above) do not
    /// belong to the Settings class.

    /// 'readonly' setting values mean:
    /// readonly = 0 - any query is allowed, client can change any setting.
    /// readonly = 1 - only readonly queries are allowed, client can't change settings.
    /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

    /// In theory if initially readonly = 0, the client can change any setting and then set readonly
    /// to some other value.
    const auto & settings = context.getSettingsRef();

    /// Only readonly queries are allowed for HTTP GET requests.
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
    {
        if (settings.readonly == 0)
            context.setSetting("readonly", 2);
    }

    bool has_external_data = startsWith(request.getContentType(), "multipart/form-data");

    if (has_external_data)
    {
        /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
        reserved_param_suffixes.reserve(3);
        /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
        reserved_param_suffixes.emplace_back("_format");
        reserved_param_suffixes.emplace_back("_types");
        reserved_param_suffixes.emplace_back("_structure");
    }

    std::string database = request.get("X-ClickHouse-Database", "");
    std::string default_format = request.get("X-ClickHouse-Format", "");

    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (key == "database")
        {
            if (database.empty())
                database = value;
        }
        else if (key == "default_format")
        {
            if (default_format.empty())
                default_format = value;
        }
        else if (param_could_be_skipped(key))
        {
        }
        else
        {
            /// Other than query parameters are treated as settings.
//            if (!customizeQueryParam(context, key, value))
//                settings_changes.push_back({key, value});
        }
    }

    if (!database.empty())
        context.setCurrentDatabase(database);

    if (!default_format.empty())
        context.setDefaultFormat(default_format);

    /// For external data we also want settings
    context.checkSettingsConstraints(settings_changes);
    context.applySettingsChanges(settings_changes);

    // onst auto & query = getQuery(request, params, context);
    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);
    in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

    /// HTTP response compression is turned on only if the client signalled that they support it
    /// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
    used_output.out->setCompression(client_supports_http_compression && settings.enable_http_compression);
    if (client_supports_http_compression)
        used_output.out->setCompressionLevel(settings.http_zlib_compression_level);

    used_output.out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (in_post_compressed && settings.http_native_compression_disable_checksumming_on_decompress)
        static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

    /// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed
    /// Origin header.
    used_output.out->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());

    auto append_callback = [&context] (ProgressCallback callback)
    {
        auto prev = context.getProgressCallback();

        context.setProgressCallback([prev, callback] (const Progress & progress)
        {
            if (prev)
                prev(progress);

            callback(progress);
        });
    };

    /// These 2 progress mode options are mutually exclusive, and
    /// send_progress_in_http_body overrides send_progress_in_http_headers
    SendProgressMode send_progress_mode = SendProgressMode::progress_none;
    if (settings.send_progress_in_http_headers)
    {
        send_progress_mode = SendProgressMode::progress_via_header;
    }

    if (settings.send_progress_in_http_body)
    {
        send_progress_mode = SendProgressMode::progress_via_body;
    }
    used_output.out->setSendProgressMode(send_progress_mode);

    /// While still no data has been sent, we will report about query execution progress
    /// by sending HTTP body or headers.
    if (send_progress_mode != SendProgressMode::progress_none)
        append_callback([&used_output] (const Progress & progress) { used_output.out->onProgress(progress); });

    if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
    {
        append_callback([&context, &request](const Progress &)
        {
            /// Assume that at the point this method is called no one is reading data from the socket any more:
            /// should be true for read-only queries.
            if (!request.checkPeerConnected())
                context.killCurrentQuery();
        });
    }

    // customizeContext(request, context);

    query_scope.emplace(context);

    executeQuery(*in, *used_output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
        [&response] (const String & current_query_id, const String & content_type, const String & format, const String & timezone)
        {
            response.setContentType(content_type);
            response.add("X-ClickHouse-Query-Id", current_query_id);
            response.add("X-ClickHouse-Format", format);
            response.add("X-ClickHouse-Timezone", timezone);
        }
    );

    if (used_output.hasDelayed())
    {
        /// TODO: set Content-Length if possible
        pushDelayedResults(used_output);
    }

    /// Add query_id into response
    Poco::JSON::Object resp;
    resp.set("query_id", client_info.initial_query_id);
    std::stringstream  resp_str_stream;
    resp.stringify( resp_str_stream, 4);
    std::string resp_str = resp_str_stream.str();

    writeString(resp_str, *used_output.out_maybe_delayed_and_compressed);
    writeChar('\n', *used_output.out_maybe_delayed_and_compressed);
    used_output.out_maybe_delayed_and_compressed->next();
    used_output.out->next();

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
    /// the client.
    used_output.out->finalize();

}

}
