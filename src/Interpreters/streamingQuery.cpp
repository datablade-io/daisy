#include "streamingQuery.h"

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

/// Daisy : starts. added by Daisy
namespace DB
{

namespace
{
void streamingTables(const ASTSelectQuery * select_ast, std::vector<String> & results)
{
    if (select_ast->stream)
    {
        results.push_back("stream");
    }

#if 0
    if (select_ast->stream)
    {
        results.push_back("streaming");
        auto tables = node->tables();
        if (!tables)
        {
            /// Throw
        }
        else
        {
            const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();
            const auto & child = tables_in_select_query.children.front();
            const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
            const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

            if (table_expr.database_and_table_name)
            {
                if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(table_expr.database_and_table_name.get()))
                {
                    auto table_id = IdentifierSemantic::extractDatabaseAndTable(identifier);
                    results.push_back();
                }
            }
        }
    }
    else
    {
    }
#endif
}

void streamingTables(const ASTSelectWithUnionQuery * select_ast, std::vector<String> & results)
{
    for (ASTs::const_iterator it = select_ast->list_of_selects->children.begin(); it != select_ast->list_of_selects->children.end(); ++it)
    {
        if (const auto * select_with_union = (*it)->as<ASTSelectWithUnionQuery>())
        {
            streamingTables(select_with_union, results);
        }
        else if (const auto * select = (*it)->as<ASTSelectQuery>())
        {
            streamingTables(select, results);
        }
    }
}
}

std::vector<String> streamingTables(const ASTPtr & ast)
{
    std::vector<String> results;

    if (const auto * select_query = ast->as<ASTSelectQuery>())
    {
        streamingTables(select_query, results);
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        streamingTables(select_with_union_query, results);
    }
    return results;
}
}
