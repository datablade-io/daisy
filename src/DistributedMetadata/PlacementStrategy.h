#pragma once

#include <common/types.h>

#include <boost/noncopyable.hpp>

#include <unordered_map>
#include <vector>

namespace DB
{
using DiskSpace = std::unordered_map<String, UInt64>; /// (policy name, free disk space)
struct NodeMetrics
{
    /// `host` is network reachable like hostname, FQDN or IP of the node
    String host;
    /// `node_identity` can be unique uuid
    String node_identity;
    /// `(policy name, free disk space)`
    DiskSpace disk_space;

    String http_port;
    String tcp_port;

    UInt64 broadcast_time;
    UInt64 monotonic_update_time;

    size_t num_of_tables;

    bool in_sync;

    explicit NodeMetrics(const String & host_) : host(host_), broadcast_time(0), monotonic_update_time(0), num_of_tables(0), in_sync(true)
    {
    }
};
using NodeMetricsPtr = std::shared_ptr<NodeMetrics>;
using NodeMetricsContainer = std::unordered_map<String, NodeMetricsPtr>;

class PlacementStrategy : private boost::noncopyable
{
public:
    struct PlacementRequest
    {
        size_t requested_nodes;
        String storage_policy;
    };

    PlacementStrategy() = default;
    virtual ~PlacementStrategy() = default;
    virtual std::vector<NodeMetricsPtr> qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request) = 0;
};

class DiskStrategy final : public PlacementStrategy
{
public:
    virtual std::vector<NodeMetricsPtr>
    qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request) override;
};

using PlacementStrategyPtr = std::shared_ptr<PlacementStrategy>;

}
