#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/TimeParam.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class AddTimeParamVisitor
{
public:
    AddTimeParamVisitor(Context & context_) : context(context_){ }

    void visit(ASTPtr ast);

private:
    Context & context;

    void visitSelectQuery(ASTPtr ast);
    void visitSelectWithUnionQuery(ASTPtr ast);

    bool containTimeField(ASTPtr table_identifier_node);
};

}
