#pragma once

#include <unordered_map>
#include <Parsers/IAST.h>

namespace DB
{
class ASTSelectQuery;
struct ASTTableExpression;

class EliminateSubqueryVisitor
{
public:
    static void visit(ASTPtr & ast);

private:
    static void visit(ASTSelectQuery & select);
    static void visit(ASTTableExpression & tableExpression, ASTSelectQuery & parent_select);
    static void rewriteColumns(ASTPtr & astPtr, std::unordered_map<String, ASTPtr> & child_select);
    static bool mergeColumns(ASTSelectQuery & parent_query, ASTSelectQuery & child_query);
};
}
