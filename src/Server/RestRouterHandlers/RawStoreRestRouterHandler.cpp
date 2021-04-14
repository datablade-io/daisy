#include "RawStoreRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
String RawStoreRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status */) const
{
    /// FIXME: Implement in another PR
    return "";
}

String RawStoreRestRouterHandler::getOrderbyExpr(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & order_by_granularity = payload->has("order_by_granularity") ? payload->get("order_by_granularity").toString() : "m";
    return granularity_func_mapping[order_by_granularity] + ",  sourcetype";
}

String RawStoreRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const
{
    const auto & database_name = getPathParameter("database");

    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
    create_segments.push_back("(");
    create_segments.push_back("_raw String COMMENT 'rawstore',");
    create_segments.push_back("_time DateTime64(3) CODEC (DoubleDelta, LZ4), ");
    create_segments.push_back("_index_time DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4), ");
    create_segments.push_back("sourcetype LowCardinality(String), ");
    create_segments.push_back("source String, ");
    create_segments.push_back("host String ");
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = " + getEngineExpr(payload));
    create_segments.push_back("PARTITION BY " + getPartitionExpr(payload, "D"));
    create_segments.push_back("ORDER BY (" + getOrderbyExpr(payload) + ")");

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
}
