#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct VerticalRichImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::VERTICAL_RICH;
    static constexpr auto Name = "extractAllGroupsVerticalRich";
};

}

namespace DB
{

void registerFunctionExtractAllGroupsVerticalRich(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAllGroups<VerticalRichImpl>>();
    factory.registerAlias("highlight", VerticalRichImpl::Name, FunctionFactory::CaseSensitive);
}

}
