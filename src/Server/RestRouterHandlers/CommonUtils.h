#pragma once

#include <Poco/JSON/Parser.h>
#include <common/types.h>

class ASTPtr;
class ContextPtr;
namespace DB
{
namespace ColumnUtils
{
    String getCreateColumnDefination(const Poco::JSON::Object::Ptr & payload);
    String getUpdateColumnDefination(const Poco::JSON::Object::Ptr & payload, String & column_name);
}

namespace QueryUtils
{
    ASTPtr parseQuerySyntax(const String & create_table_query, ContextPtr query_context);
    String processQuery(const String & query, ContextPtr query_context);
}

}
