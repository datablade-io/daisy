#include "SQLAnalyzeHTTPHandler.h"

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/QueryProcessingStage.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/copyData.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSetRoleQuery.h>
#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Parsers/ASTShowAccessQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTShowPrivilegesQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/parseQueryPipe.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/QueryProfileVisitor.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int REQUIRED_PASSWORD;
}

namespace
{

std::string buildJSONResponse(
    const std::string & original_query,
    const std::string & rewritten_query,
    const std::string & query_type,
    const QueryProfileMatcher::Data & query_profile,
    const Block & sampleBlock,
    const std::set<std::tuple<std::string, std::string, bool, std::string, std::string>> & required_columns)
{
    /// {
    ///    "required_columns": [
    ///        {"database": <database>, "table": <table>, "is_view": false, "column": <column>, "column_type": <type>},
    ///        ...
    ///    ],
    ///    "result_columns": {
    ///        {"column": <column>, "column_type": <type>},
    ///        ...
    ///    },
    ///    "rewritten_query": <query>,
    ///    "original_query": <query>,
    ///    "query_type": CREATE | SELECT | INSERT INTO | ...
    ///    "has_aggr": true,
    ///    "has_table_join": true,
    ///    "has_union": true,
    ///    "has_subquery": true
    /// }

    Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
    result->set("original_query", original_query);
    result->set("rewritten_query", rewritten_query);
    result->set("query_type", query_type);
    result->set("has_aggr", query_profile.has_aggr);
    result->set("has_table_join", query_profile.has_table_join);
    result->set("has_union", query_profile.has_union);
    result->set("has_subquery", query_profile.has_subquery);

    /// Required columns
    int i = 0;
    Poco::JSON::Array::Ptr required_columns_obj = new Poco::JSON::Array();
    for (auto & columnInfo : required_columns)
    {
        Poco::JSON::Object::Ptr column = new Poco::JSON::Object();
        column->set("database", std::get<0>(columnInfo));
        column->set("table", std::get<1>(columnInfo));
        column->set("is_view", std::get<2>(columnInfo));
        column->set("column", std::get<3>(columnInfo));
        column->set("column_type", std::get<4>(columnInfo));
        required_columns_obj->set(i++, column);
    }
    result->set("required_columns", required_columns_obj);

    /// Result columns
    i = 0;
    Poco::JSON::Array::Ptr result_columns_obj = new Poco::JSON::Array();
    for (const auto & column_info: sampleBlock)
    {
        Poco::JSON::Object::Ptr column = new Poco::JSON::Object();
        column->set("column", column_info.name);
        column->set("column_type", column_info.type->getName());
        result_columns_obj->set(i++, column);
    }
    result->set("result_columns", result_columns_obj);

    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::condense(result, oss);
    return oss.str();
}

std::string queryType(ASTPtr & ast)
{
    if (ast->as<ASTSelectQuery>())
        return "SELECT";
    else if (ast->as<ASTSelectWithUnionQuery>())
        return "SELECT";
    else if (ast->as<ASTInsertQuery>())
        return "INSERT";
    else if (ast->as<ASTCreateQuery>())
        return "CREATE";
    else if (ast->as<ASTDropQuery>())
        return "DROP";
    else if (ast->as<ASTRenameQuery>())
        return "RENAME";
    else if (ast->as<ASTShowTablesQuery>())
        return "SHOW_TABLE";
    else if (ast->as<ASTUseQuery>())
        return "USE";
    else if (ast->as<ASTSetQuery>())
        return "SET";
    else if (ast->as<ASTSetRoleQuery>())
        return "SET_ROLE";
    else if (ast->as<ASTOptimizeQuery>())
        return "OPTIMIZE";
    else if (ast->as<ASTExistsTableQuery>())
        return "EXISTS_TABLE";
    else if (ast->as<ASTExistsDictionaryQuery>())
        return "EXISTS_DICT";
    else if (ast->as<ASTShowCreateTableQuery>())
        return "SHOW_CREATE_TABLE";
    else if (ast->as<ASTShowCreateDatabaseQuery>())
        return "SHOW_CREATE_DATABASE";
    else if (ast->as<ASTShowCreateDictionaryQuery>())
        return "SHOW_CREATE_DICT";
    else if (ast->as<ASTDescribeQuery>())
        return "DESCRIBE";
    else if (ast->as<ASTExplainQuery>())
        return "EXPLAIN";
    else if (ast->as<ASTShowProcesslistQuery>())
        return "SHOW_PROC_LIST";
    else if (ast->as<ASTAlterQuery>())
        return "ALTER";
    else if (ast->as<ASTCheckQuery>())
        return "CHECK";
    else if (ast->as<ASTKillQueryQuery>())
        return "KILL";
    else if (ast->as<ASTSystemQuery>())
        return "SYSTEM";
    else if (ast->as<ASTWatchQuery>())
        return "WATCH";
    else if (ast->as<ASTCreateUserQuery>())
        return "CREATE_USER";
    else if (ast->as<ASTCreateRoleQuery>())
        return "CREATE_ROLE";
    else if (ast->as<ASTCreateQuotaQuery>())
        return "CREATE_QUOTA";
    else if (ast->as<ASTCreateRowPolicyQuery>())
        return "CREATE_ROW_POLICY";
    else if (ast->as<ASTCreateSettingsProfileQuery>())
        return "CREATE_SETTINGS_PROFILE";
    else if (ast->as<ASTDropAccessEntityQuery>())
        return "DROP_ACCESS_ENTITY";
    else if (ast->as<ASTGrantQuery>())
        return "GRANT";
    else if (ast->as<ASTShowCreateAccessEntityQuery>())
        return "SHOW_CREATE_ACCESS_ENTITY";
    else if (ast->as<ASTShowGrantsQuery>())
        return "SHOW_GRANT";
    else if (ast->as<ASTShowAccessEntitiesQuery>())
        return "SHOW_ACCESS_ENTITIES";
    else if (ast->as<ASTShowAccessQuery>())
        return "SHOW_ACCESS";
    else if (ast->as<ASTShowPrivilegesQuery>())
        return "SHOW_PRIV";
    else if (ast->as<ASTExternalDDLQuery>())
        return "EXTERNAL_DDL";
    else
        return "UNKNOWN";
}

void setupAuth(const Poco::Net::HTTPServerRequest & request, const HTMLForm & params, Context & context)
{
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
            throw Exception(
                "Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods "
                "simultaneously",
                ErrorCodes::REQUIRED_PASSWORD);
        }
    }

    context.setUser(user, password, request.clientAddress());
    if (!quota_key.empty())
        context.setQuotaKey(quota_key);
}

void setupQueryParams(const HTMLForm & params, Context & context)
{
    static const NameSet reserved_param_names{
        "compress",
        "decompress",
        "user",
        "password",
        "quota_key",
        "query_id",
        "stacktrace",
        "buffer_size",
        "wait_end_of_query",
        "session_id",
        "session_timeout",
        "session_check",
        "database",
        "default_format"};

    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (key.empty())
            continue;

        if (reserved_param_names.count(key))
            continue;

        if (key == "query")
            continue;

        if (startsWith(key, "param_"))
        {
            /// Save name and values of substitution in dictionary.
            const String parameter_name = key.substr(strlen("param_"));
            context.setQueryParameter(parameter_name, value);
            continue;
        }


        settings_changes.push_back({key, value});
    }

    context.checkSettingsConstraints(settings_changes);
    context.applySettingsChanges(settings_changes);
}

std::string getQuery(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params)
{
    std::string param_name = "query";
    if (likely(!startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Part of the query can be passed in the 'query' parameter and the rest in the request body
        /// (http method need not necessarily be POST). In this case the entire query consists of the
        /// contents of the 'query' parameter, a line break and the request body.
        std::string query_param = params.get(param_name, "");
        if (!query_param.empty())
        {
            return query_param;
        }
    }

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    std::unique_ptr<ReadBuffer> in = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromIStream>(request.stream()), chooseCompressionMethod({}, http_request_compression_method_str));

    if (!in->hasPendingData())
        in->next();

    const auto & settings = context.getSettingsRef();

    PODArray<char> parse_buf;
    WriteBufferFromVector<PODArray<char>> out(parse_buf);
    LimitReadBuffer limit(*in, settings.max_query_size + 1, false);
    copyData(limit, out);
    out.finalize();

    return std::string(parse_buf.data(), parse_buf.data() + parse_buf.size());
}
}

SQLAnalyzeHTTPHandler::SQLAnalyzeHTTPHandler(IServer & server_, const std::string & name)
    : server(server_)
    , log(&Poco::Logger::get(name))
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


void SQLAnalyzeHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("SQLAnalyzer");
    ThreadStatus thread_status;

    Context context = server.context();
    CurrentThread::QueryScope scope{context};
    context.setCollectRequiredColumns(true);

    response.setContentType("text/plain; charset=UTF-8");
    response.set("X-ClickHouse-Server-Display-Name", server_display_name);

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    try
    {
        HTMLForm params(request);

        setupAuth(request, params, context);
        setupQueryParams(params, context);

        /// setup query_id
        const auto & query_id = params.get("query_id", request.get("X-ClickHouse-Query-Id", ""));
        context.setCurrentQueryId(query_id);

        with_stacktrace = params.getParsed<bool>("stacktrace", false);

        processQuery(context, request, params, response);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
        {
            /// We have not sent anything yet and we don't even know if we need to compress response.
            if (with_stacktrace)
            {
                response.send() << getCurrentExceptionMessage(false) << std::endl;
            }
            else
            {
                response.send() << "failed to rewrite query" << std::endl;
            }
        }
    }

    LOG_DEBUG(log, "Done query rewrite processing");
}

void SQLAnalyzeHTTPHandler::processQuery(
    Context & context,
    Poco::Net::HTTPServerRequest & request,
    HTMLForm & params,
    Poco::Net::HTTPServerResponse & response)
{
    auto query_id = context.getCurrentQueryId();

    LOG_TRACE(log, "Request URI: {}", request.getURI());

    const auto & settings = context.getSettingsRef();
    std::string error_message;

    auto query = getQuery(context, request, params);

    LOG_INFO(log, "query rewrite, query_id={} origin={}", query_id, query);

    ParserQuery parser(query.c_str() + query.size());
    auto res = rewriteQueryPipeAndParse(
        parser, query.c_str(), query.c_str() + query.size(), error_message, false, settings.max_query_size, settings.max_parser_depth);

    response.add("X-ClickHouse-Query-Id", query_id);

    if (error_message.empty())
    {
        auto & [rewritten_query, ast] = res;

        LOG_INFO(log, "query rewrite, query_id={} rewritten={}", query_id, rewritten_query);

        QueryProfileMatcher::Data profile;
        QueryProfileVisitor visitor(profile);
        visitor.visit(ast);

        Block block;

        /// FIXME: CREATE TABLE ... AS SELECT ...
        /// FIXME: INSERT INTO TABLE ... SELECT ...
        if (ast->as<ASTSelectWithUnionQuery>())
        {
            /// interpreter will trigger ast analysis. One side effect is collecting
            /// required columns during the analysis process
            InterpreterSelectWithUnionQuery interpreter(ast, context, SelectQueryOptions());
            block = interpreter.getSampleBlock();
        }

        auto query_type = queryType(ast);
        auto result = buildJSONResponse(query, rewritten_query, query_type, profile, block, context.requiredColumns());

        /// send back response
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPResponse::HTTP_OK);
        response.send() << result << std::endl;
    }
    else
    {
        LOG_ERROR(log, "query rewrite, query_id={} error_message={}", query_id, error_message);

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPResponse::HTTP_BAD_REQUEST);
        response.send() << error_message << std::endl;
    }
}

HTTPRequestHandlerFactoryPtr createSQLAnalyzeHandlerFactory(IServer & server, const std::string & config_prefix)
{
    (void)server;
    (void)config_prefix;
    /*auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<SQLAnalyzeHTTPHandler>>(server, "sqlanalyze");
    factory->addFiltersFromConfig(server.config(), config_prefix);
    return factory;*/
    return nullptr;
}
}
