#include "ColumnRestRouterHandler.h"
#include "CommonUtils.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DistributedMetadata/CatalogService.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>

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
}

bool ColumnRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

bool ColumnRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(UPDATE_SCHEMA, payload, error_msg);
}

String ColumnRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
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
    create_segments.push_back(ColumnUtils::getCreateColumnDefination(payload));
    const String & query = boost::algorithm::join(create_segments, " ");

    return QueryUtils::processQuery(query, query_context);
}

String ColumnRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
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

    const String & query = "ALTER TABLE " + database_name + "." + table_name + " " + ColumnUtils::getUpdateColumnDefination(payload, column_name);

    return QueryUtils::processQuery(query, query_context);
}

String ColumnRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & /*http_status*/) const
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

    return QueryUtils::processQuery(query, query_context);
}

bool ColumnRestRouterHandler::columnExist(const String & database_name, const String & table_name, const String & column_name) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & tables = catalog_service.findTableByName(database_name, table_name);

    if (tables.size() == 0)
    {
        return false;
    }

    const auto & query_ptr = QueryUtils::parseQuerySyntax(tables[0]->create_table_query, query_context);
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
}
