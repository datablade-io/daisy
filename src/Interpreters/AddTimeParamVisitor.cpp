#include <Interpreters/AddTimeParamVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/TimeParam.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>


namespace DB
{
bool AddTimeParamVisitor::containTimeField(ASTPtr node)
{
    if (ASTIdentifier * table_identifier_node = node->as<ASTIdentifier>())
    {
        StorageID storage_id(*table_identifier_node);
        auto table_id = context.resolveStorageID(storage_id);
        auto db = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        StoragePtr table = db->tryGetTable(table_id.table_name, context);
        if (table)
        {
            auto metadata = table->getInMemoryMetadataPtr();
            const auto & col_desc = metadata->getColumns();
            return col_desc.has("_time") && col_desc.get("_time").type->getTypeId() == TypeIndex::DateTime64;
        }
    }
    return false;
}

void AddTimeParamVisitor::visitSelectQuery(ASTPtr ast)
{
    if (ASTSelectQuery * select = ast->as<ASTSelectQuery>())
    {
        ASTPtr tables = select->tables();
        if (!tables->children.empty())
        {
            ASTPtr node = tables->children[0];
            if (ASTTablesInSelectQueryElement * first_table = node->as<ASTTablesInSelectQueryElement>())
            {
                ASTTableExpression * table_expression = first_table->table_expression->as<ASTTableExpression>();
                if (table_expression->database_and_table_name)
                {
                    ParserExpressionWithOptionalAlias elem_parser(false);
                    if (containTimeField(table_expression->database_and_table_name))
                    {
                        /// merge time picker predicates into the where subtree of this select node
                        /// BE Careful: where_statement may be null, when the sql doesn't contain where expression
                        ASTPtr where_statement = select->where();
                        ASTPtr new_node;
                        if (!context.getTimeParam().getStart().empty())
                        {
                            new_node = parseQuery(
                                elem_parser,
                                context.getTimeParam().getStart(),
                                context.getSettingsRef().max_query_size,
                                context.getSettingsRef().max_parser_depth);
                            new_node = makeASTFunction("greaterOrEquals", std::make_shared<ASTIdentifier>("_time"), new_node);
                        }
                        if (!context.getTimeParam().getEnd().empty())
                        {
                            ASTPtr less = parseQuery(
                                elem_parser,
                                context.getTimeParam().getEnd(),
                                context.getSettingsRef().max_query_size,
                                context.getSettingsRef().max_parser_depth);
                            less = makeASTFunction("less", std::make_shared<ASTIdentifier>("_time"), less);
                            new_node = new_node ? makeASTFunction("and", less, new_node) : less;
                        }
                        where_statement = where_statement ? makeASTFunction("and", new_node, where_statement) : new_node;
                        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_statement));
                    }
                }
                else if (table_expression->subquery)
                {
                    ASTPtr subquery = table_expression->subquery->children[0];
                    if (subquery->as<ASTSelectWithUnionQuery>())
                    {
                        visitSelectWithUnionQuery(subquery);
                    }
                    else if (subquery->as<ASTSelectQuery>())
                    {
                        visitSelectQuery(subquery);
                    }
                }
            }
        }
    }
}

void AddTimeParamVisitor::visitSelectWithUnionQuery(ASTPtr ast)
{
    if (const ASTSelectWithUnionQuery * un = ast->as<ASTSelectWithUnionQuery>())
    {
        if (un->list_of_selects->children.empty())
        {
            return;
        }
        if (un->list_of_selects->children[0]->as<ASTSelectQuery>())
            visitSelectQuery(un->list_of_selects->children[0]);
    }
}

void AddTimeParamVisitor::visit(ASTPtr ast)
{
    if (ast->as<ASTSelectQuery>())
    {
        visitSelectQuery(ast);
    }
    else if (ast->as<ASTSelectWithUnionQuery>())
    {
        visitSelectWithUnionQuery(ast);
    }
}

}
