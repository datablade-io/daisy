#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/TimePicker.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
struct TimePicker;
class Context;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class AddTimePickerVisitor
{
public:
    AddTimePickerVisitor(Context & _context) : context(_context), time_picker(_context.getTimePicker()) { }

    void visit(ASTPtr ast);

private:
    Context & context;
    const TimePicker & time_picker;

    template <typename T>
    bool instanceof (const ASTPtr ast) const
    {
        return dynamic_cast<const T *>(ast.get()) != nullptr;
    }

    template <typename T>
    bool instanceof (const IAST * ast_ptr) const
    {
        return dynamic_cast<const T *>(ast_ptr) != nullptr;
    }

    void visitSelectQuery(ASTPtr ast);
    void visitSelectWithUnionQuery(ASTPtr ast);

    bool containTimeField(ASTPtr table_identifier_node);
};

}
