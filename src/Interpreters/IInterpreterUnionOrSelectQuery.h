#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class IInterpreterUnionOrSelectQuery : public IInterpreter
{
public:
    IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
        : query_ptr(query_ptr_)
        , context(Context::createCopy(context_))
        , options(options_)
        , max_streams(context->getSettingsRef().max_threads)
    {
    }

    virtual void buildQueryPlan(QueryPlan & query_plan) = 0;

    virtual void ignoreWithTotals() = 0;

    virtual ~IInterpreterUnionOrSelectQuery() override = default;

    Block getSampleBlock() { return result_header; }

    size_t getMaxStreams() const { return max_streams; }

    /// Daisy : starts
    virtual bool hasAggregation() const = 0;
    /// Daisy : ends

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override;

protected:
    ASTPtr query_ptr;
    ContextPtr context;
    Block result_header;
    SelectQueryOptions options;
    size_t max_streams = 1;
    bool settings_limit_offset_needed = false;
    bool settings_limit_offset_done = false;
};
}

