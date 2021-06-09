#include "streamingQuery.h"

#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

/// Daisy : starts. added by Daisy
namespace DB
{

namespace ErrorCodes
{
    const extern int INCORRECT_QUERY;
}

namespace
{
void streamingTables(const ASTSelectQuery * select_ast, std::vector<String> & results, const String & current_db)
{
    if (!select_ast->stream)
    {
        return;
    }

    auto tables = select_ast->tables();
    if (!tables)
    {
        throw Exception("STREAM query is only valid on DistributedMergeTree table", ErrorCodes::INCORRECT_QUERY);
    }

    const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();
    const auto & child = tables_in_select_query.children.front();
    const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
    const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

    if (table_expr.database_and_table_name)
    {
        if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(table_expr.database_and_table_name.get()))
        {
            auto table_id = IdentifierSemantic::extractDatabaseAndTable(*identifier);
            if (table_id.database_name.empty())
            {
                table_id.database_name = current_db;
            }

            results.push_back(table_id.getFullTableName());
        }
        else
        {
            throw Exception("STREAM query is only valid on DistributedMergeTree table", ErrorCodes::INCORRECT_QUERY);
        }
    }
    else
    {
        throw Exception("STREAM query is only valid on DistributedMergeTree table", ErrorCodes::INCORRECT_QUERY);
    }
}

void streamingTables(
    const ASTSelectWithUnionQuery * select_ast, std::vector<String> & results, const String & current_db, bool & has_union_selects)
{
    if (select_ast->list_of_selects->children.size() > 1)
    {
        has_union_selects = true;
    }

    for (ASTs::const_iterator it = select_ast->list_of_selects->children.begin(); it != select_ast->list_of_selects->children.end(); ++it)
    {
        if (const auto * select_with_union = (*it)->as<ASTSelectWithUnionQuery>())
        {
            streamingTables(select_with_union, results, current_db, has_union_selects);
        }
        else if (const auto * select = (*it)->as<ASTSelectQuery>())
        {
            streamingTables(select, results, current_db);
        }
    }
}
}

std::vector<String> streamingTables(const ASTPtr & ast, const String & current_db, bool enforce_single_streaming_select)
{
    std::vector<String> results;

    bool has_union_selects = false;

    if (const auto * select_query = ast->as<ASTSelectQuery>())
    {
        streamingTables(select_query, results, current_db);
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        streamingTables(select_with_union_query, results, current_db, has_union_selects);
    }

    if (enforce_single_streaming_select && has_union_selects && !results.empty())
    {
        throw Exception("STREAM query only supports single DistributedMergeTree table select", ErrorCodes::INCORRECT_QUERY);
    }

    if (enforce_single_streaming_select && !results.empty())
    {
        assert(results.size() == 1);
    }
    return results;
}
}
