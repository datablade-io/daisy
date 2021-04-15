#include "TabularTableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int CONFIG_ERROR;
    extern const int OK;
}

std::map<String, std::map<String, String> > TabularTableRestRouterHandler::create_schema= {
    {"required",{
                    {"name","string"},
                    {"columns", "array"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {"_time_column", "string"},
                    {"replication_factor", "int"},
                    {"order_by_expression", "string"},
                    {"order_by_granularity", "string"},
                    {"partition_by_granularity", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > TabularTableRestRouterHandler::column_schema = {
    {"required",{
                    {"name","string"},
                    {"type", "string"},
                }
    },
    {"optional", {
                    {"nullable", "bool"},
                    {"default", "string"},
                    {"compression_codec", "string"},
                    {"ttl_expression", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

bool TabularTableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
    {
        return false;
    }

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (const auto & col : *columns)
    {
        if (!validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>(), error_msg))
        {
            return false;
        }
    }

    return TableRestRouterHandler::validatePost(payload, error_msg);
}

inline String TabularTableRestRouterHandler::getTimeColumn(const Poco::JSON::Object::Ptr & payload) const
{
    return payload->has("_time_column") ? payload->get("_time_column").toString() : "_time";
}

String TabularTableRestRouterHandler::getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/) const
{
    const auto & order_by_granularity = payload->has("order_by_granularity") ? payload->get("order_by_granularity").toString() : "D";
    const auto & default_order_expr = granularity_func_mapping[order_by_granularity];
    const auto & order_by_expression = payload->has("order_by_expression") ? payload->get("order_by_expression").toString() : String();

    if (order_by_expression.empty())
    {
        return default_order_expr;
    }

    /// FIXME: We may need to check whether the time column is already set as the first column in order by expression.

    return default_order_expr + ", " + order_by_expression;
}

String TabularTableRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const
{
    const auto & database_name = getPathParameter("database");
    const auto & time_col = getTimeColumn(payload);
    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back(getColumnsDefinition(payload));
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = " + getEngineExpr(payload));
    create_segments.push_back("PARTITION BY " + getPartitionExpr(payload, "M"));
    create_segments.push_back("ORDER BY (" + getOrderbyExpr(payload, time_col) + ")");

    if (payload->has("ttl_expression"))
    {
        /// FIXME  Enforce time based TTL only
        create_segments.push_back("TTL " + payload->get("ttl_expression").toString());
    }

    if (!shard.empty())
    {
        create_segments.push_back("SETTINGS shard=" + shard);
    }

    return boost::algorithm::join(create_segments, " ");
}

String TabularTableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & columns = payload->getArray("columns");

    std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    using std::begin;
    using std::end;
    std::vector<String> column_definitions;

    for (const auto & col : *columns)
    {
        column_definitions.push_back(getColumnDefinition(col.extract<Poco::JSON::Object::Ptr>()));
    }

    std::copy(begin(column_definitions), end(column_definitions), std::ostream_iterator<String>(oss, ","));
    if (payload->has("_time_column"))
    {
        return oss.str() + " `_time` DateTime64(3) DEFAULT " + payload->get("_time_column").toString();
    }
    return oss.str() + " `_time` DateTime64(3, UTC) DEFAULT now64(3)";
}

String TabularTableRestRouterHandler::getColumnDefinition(const Poco::JSON::Object::Ptr & column) const
{
    std::vector<String> create_segments;

    create_segments.push_back(column->get("name").toString());
    if (column->has("nullable") && column->get("nullable"))
    {
        create_segments.push_back(" Nullable(" + column->get("type").toString() + ")");
    }
    else
    {
        create_segments.push_back(" " + column->get("type").toString());
    }

    if (column->has("default"))
    {
        create_segments.push_back(" DEFAULT " + column->get("default").toString());
    }

    if (column->has("compression_codec"))
    {
        create_segments.push_back(" CODEC(" + column->get("compression_codec").toString() + ")");
    }

    if (column->has("ttl_expression"))
    {
        create_segments.push_back(" TTL " + column->get("ttl_expression").toString());
    }

    if (column->has("skipping_index_expression"))
    {
        create_segments.push_back(", " + column->get("skipping_index_expression").toString());
    }

    return boost::algorithm::join(create_segments, " ");
}

}
