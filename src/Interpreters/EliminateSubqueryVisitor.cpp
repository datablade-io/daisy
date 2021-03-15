#include <Interpreters/EliminateSubqueryVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
void EliminateSubqueryVisitor::visit(ASTPtr & ast)
{
    if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & select : select_with_union_query->list_of_selects->children)
        {
            if (auto * select_query = select->as<ASTSelectQuery>())
            {
                visit(*select_query);
            }
        }
    }
    else
    {
        for (auto & child : ast->children)
        {
            visit(child);
        }
    }
}

void EliminateSubqueryVisitor::visit(ASTSelectQuery & select_query)
{
    /// ignore "join" case
    if (select_query.tables() == nullptr || select_query.tables()->children.size() != 1)
    {
        return;
    }

    for (auto & table : select_query.tables()->children)
    {
        if (auto * tableElement = table->as<ASTTablesInSelectQueryElement>())
        {
            if (auto * tableExpression = tableElement->table_expression->as<ASTTableExpression>())
            {
                visit(*tableExpression, select_query);
            }
        }
    }
}

void EliminateSubqueryVisitor::visit(ASTTableExpression & tableExpression, ASTSelectQuery & parent_select)
{
    if (tableExpression.subquery == nullptr)
    {
        return;
    }
    if (tableExpression.subquery->children.size() != 1)
    {
        return;
    }

    auto & select = tableExpression.subquery->children.at(0);
    if (auto * select_with_union_query = select->as<ASTSelectWithUnionQuery>())
    {
        if (select_with_union_query->list_of_selects->children.size() != 1)
        {
            return;
        }
        if (auto * sub_query = select_with_union_query->list_of_selects->children.at(0)->as<ASTSelectQuery>())
        {
            /// handle sub query in table expression recursively
            visit(*sub_query);

            if (sub_query->groupBy() || sub_query->having() || sub_query->orderBy() || sub_query->limitBy() || sub_query->limitByLength()
                || sub_query->limitByOffset() || sub_query->limitLength() || sub_query->limitOffset() || sub_query->distinct
                || sub_query->with())
                return;

            /// try to eliminate subquery
            if (!mergeColumns(parent_select, *sub_query))
            {
                return;
            }

            if (sub_query->where() && parent_select.where())
            {
                auto where = makeASTFunction("and", sub_query->where()->clone(), parent_select.where()->clone());
                parent_select.setExpression(ASTSelectQuery::Expression::WHERE, where);
            }
            else if (sub_query->where())
            {
                parent_select.setExpression(ASTSelectQuery::Expression::WHERE, sub_query->where()->clone());
            }

            if (sub_query->prewhere() && parent_select.prewhere())
            {
                auto prewhere = makeASTFunction("and", sub_query->prewhere()->clone(), parent_select.prewhere()->clone());
                parent_select.setExpression(ASTSelectQuery::Expression::PREWHERE, prewhere);
            }
            else if (sub_query->prewhere())
            {
                parent_select.setExpression(ASTSelectQuery::Expression::PREWHERE, sub_query->prewhere()->clone());
            }

            parent_select.setExpression(ASTSelectQuery::Expression::TABLES, sub_query->tables()->clone());

            return;
        }
    }
}

void EliminateSubqueryVisitor::rewriteColumns(ASTPtr & astPtr, std::unordered_map<String, ASTPtr> & subquery_selects)
{
    if (auto * identifier = astPtr->as<ASTIdentifier>())
    {
        auto it = subquery_selects.find(identifier->name());
        if (it != subquery_selects.end())
        {
            astPtr = it->second->clone();
            astPtr->setAlias("");
        }
    }
    else
    {
        for (auto & child : astPtr->children)
        {
            rewriteColumns(child, subquery_selects);
        }
    }
}

bool EliminateSubqueryVisitor::mergeColumns(ASTSelectQuery & parent_query, ASTSelectQuery & child_query)
{
    /// select sum(b) from (select id as b from table)
    std::unordered_map<std::string, ASTPtr> subquery_selects;
    for (auto & column : child_query.select()->children)
    {
        if (column->as<ASTAsterisk>() || column->as<ASTQualifiedAsterisk>())
        {
            continue;
        }
        else if (column->as<ASTIdentifier>())
        {
            subquery_selects.emplace(column->getAliasOrColumnName(), column);
        }
        else
        {
            return false;
        }
    }
    /// try to merge select columns
    for (auto & parent_select_item : parent_query.select()->children)
    {
        rewriteColumns(parent_select_item, subquery_selects);
    }
    return true;
}


}
