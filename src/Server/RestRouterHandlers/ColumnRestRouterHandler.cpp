#include "ColumnRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DistributedMetadata/CatalogService.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#if !defined(ARCADIA_BUILD)
#    include <Parsers/New/parseQuery.h> // Y_IGNORE
#endif

#include <boost/algorithm/string/join.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{
std::map<String, std::map<String, String> > CREATE_SCHEMA = {
    {"required",{
                    {"name","string"},
                    {"type", "string"},
                }
    },
    {"optional", {
                    {"nullable", "bool"},
                    {"default", "string"},
                    {"compression_codec", "string"},
                    {"ttl_expression", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > UPDATE_SCHEMA = {
    {"required",{
                }
    },
    {"optional", {
                    {"name", "string"},
                    {"comment", "string"},
                    {"type", "string"},
                    {"ttl_expression", "string"},
                    {"default", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

String getCreateColumnDefination(const Poco::JSON::Object::Ptr & payload)
{
    std::vector<String> create_segments;
    create_segments.push_back(payload->get("name"));

    if (payload->has("nullable") && payload->get("nullable"))
    {
        create_segments.push_back(" Nullable(" + payload->get("type").toString() + ")");
    }
    else
    {
        create_segments.push_back(" " + payload->get("type").toString());
    }

    if (payload->has("default"))
    {
        create_segments.push_back(" DEFAULT " + payload->get("default").toString());
    }

    if (payload->has("compression_codec"))
    {
        create_segments.push_back(" CODEC(" + payload->get("compression_codec").toString() + ")");
    }

    if (payload->has("ttl_expression"))
    {
        create_segments.push_back(" TTL " + payload->get("ttl_expression").toString());
    }

    if (payload->has("skipping_index_expression"))
    {
        create_segments.push_back(", " + payload->get("skipping_index_expression").toString());
    }

    return boost::algorithm::join(create_segments, " ");
}

String getUpdateColumnDefination(const Poco::JSON::Object::Ptr & payload, String & column_name)
{
    std::vector<String> update_segments;
    if (payload->has("name"))
    {
        update_segments.push_back(" RENAME COLUMN " + column_name + " TO " + payload->get("name").toString());
        column_name = payload->get("name").toString();
    }

    if (payload->has("comment"))
    {
        update_segments.push_back(" COMMENT COLUMN " + column_name + " COMMENT " + payload->get("comment").toString());
    }

    if (payload->has("type"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " " + payload->get("type").toString());
    }

    if (payload->has("default"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " DEFAULT " + payload->get("default").toString());
    }

    if (payload->has("ttl_expression"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " TTL " + payload->get("ttl_expression").toString());
    }

    if (payload->has("compression_codec"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " CODEC(" + payload->get("compression_codec").toString() + ")");
    }

    return boost::algorithm::join(update_segments, ",");
}

}

bool ColumnRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

bool ColumnRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(UPDATE_SCHEMA, payload, error_msg);
}

String ColumnRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");
    const String & column_name = payload->get("name");

    if (columnExist(database_name, table_name, column_name))
    {
        return jsonErrorResponse(fmt::format("Column {} already exists.", column_name), ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
    }

    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
        query_context->setQueryParameter("query_method", Poco::Net::HTTPRequest::HTTP_POST);
        query_context->setDistributedDDLOperation(true);
    }

    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);
    create_segments.push_back("ADD COLUMN ");
    create_segments.push_back(getCreateColumnDefination(payload));
    const String & query = boost::algorithm::join(create_segments, " ");

    return processQuery(query, http_status);
}

String ColumnRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String column_name = getPathParameter("column");
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    if (!columnExist(database_name, table_name, column_name))
    {
        return jsonErrorResponse(fmt::format("Column {} does not exist.", column_name), ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
    }

    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
        query_context->setQueryParameter("column", column_name);
        query_context->setQueryParameter("query_method", Poco::Net::HTTPRequest::HTTP_PATCH);
        query_context->setDistributedDDLOperation(true);
    }

    const String & query = "ALTER TABLE " + database_name + "." + table_name + " " + getUpdateColumnDefination(payload, column_name);

    return processQuery(query, http_status);
}

String ColumnRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    const String & column_name = getPathParameter("column");
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    if (!columnExist(database_name, table_name, column_name))
    {
        return jsonErrorResponse(fmt::format("Column {} does not exist.", column_name), ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
    }

    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context->setQueryParameter("_payload", "{}");
        query_context->setQueryParameter("column", column_name);
        query_context->setQueryParameter("query_method", Poco::Net::HTTPRequest::HTTP_DELETE);
        query_context->setDistributedDDLOperation(true);
    }

    String query = "ALTER TABLE " + database_name + "." + table_name + " DROP COLUMN " + column_name;

    return processQuery(query, http_status);
}

String ColumnRestRouterHandler::processQuery(const String & query, Int32 & /* http_status */) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    if (io.pipeline.initialized())
    {
        return "TableRestRouterHandler execute io.pipeline.initialized not implemented";
    }
    io.onFinish();

    return buildResponse();
}

String ColumnRestRouterHandler::buildResponse() const
{
    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

bool ColumnRestRouterHandler::columnExist(const String & database_name, const String & table_name, const String & column_name) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & tables = catalog_service.findTableByName(database_name, table_name);

    if (tables.size() == 0)
    {
        return false;
    }

    const auto & query_ptr = parseQuerySyntax(tables[0]->create_table_query);
    const auto & create = query_ptr->as<const ASTCreateQuery &>();
    const auto & columns_ast = create.columns_list->columns;

    for (auto ast_it = columns_ast->children.begin(); ast_it != columns_ast->children.end(); ++ast_it)
    {
        const auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();
        if (col_decl.name == column_name)
        {
            return true;
        }
    }

    return false;
}

ASTPtr ColumnRestRouterHandler::parseQuerySyntax(const String & create_table_query) const
{
    const size_t & max_query_size = query_context->getSettingsRef().max_query_size;
    const auto & max_parser_depth = query_context->getSettingsRef().max_parser_depth;
    const char * begin = create_table_query.data();
    const char * end = create_table_query.data() + create_table_query.size();

    ASTPtr ast;

#if !defined(ARCADIA_BUILD)
    if (query_context->getSettingsRef().use_antlr_parser)
    {
        ast = parseQuery(begin, end, max_query_size, max_parser_depth, query_context->getCurrentDatabase());
    }
    else
    {
        ParserQuery parser(end);
        ast = parseQuery(parser, begin, end, "", max_query_size, max_parser_depth);
    }
#else
    ParserQuery parser(end);
    ast = parseQuery(parser, begin, end, "", max_query_size, max_parser_depth);
#endif

    return ast;
}

}
