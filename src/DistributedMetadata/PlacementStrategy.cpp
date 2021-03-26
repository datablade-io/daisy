#include "PlacementStrategy.h"


namespace DB
{
std::vector<NodeMetricsPtr> DiskStrategy::qualifiedNodes(const NodeMetricsContainer & nodes_metrics, const PlacementRequest & request)
{
    if (request.requested_nodes <= 0)
    {
        return {};
    }

    std::vector<NodeMetricsPtr> qualified_nodes;
    qualified_nodes.reserve(nodes_metrics.size());

    for (const auto & [node, metrics] : nodes_metrics)
    {
        auto iter = metrics->disk_space.find(request.storage_policy);
        if (iter != metrics->disk_space.end() && iter->second > 0)
        {
            qualified_nodes.emplace_back(metrics);
        }
    }

    if (qualified_nodes.size() < request.requested_nodes)
    {
        return {};
    }

    std::sort(std::begin(qualified_nodes), std::end(qualified_nodes), [&](const auto & a, const auto & b) {
        auto a_disk_size = a->disk_space.find(request.storage_policy)->second;
        auto b_disk_size = b->disk_space.find(request.storage_policy)->second;
        return a_disk_size > b_disk_size;
    });

    return {qualified_nodes.begin(), qualified_nodes.begin() + request.requested_nodes};
}

}
