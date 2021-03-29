#include <DistributedMetadata/PlacementStrategy.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>


using namespace DB;

TEST(PlacementService, PlaceNodes)
{
    std::vector<int> default_disks;
    std::vector<int> cold_disks;
    for (int i = 0; i < 100; i++)
    {
        default_disks.push_back(i + 1);
        cold_disks.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(default_disks.begin(), default_disks.end(), g);
    std::shuffle(cold_disks.begin(), cold_disks.end(), g);

    NodeMetricsContainer container;
    String default_policy = "default";
    String cold_policy = "cold_policy";
    for (int i = 0; i < 100; i++)
    {
        String node = std::to_string(i);
        NodeMetricsPtr nodeMetrics = std::make_shared<NodeMetrics>(node);
        nodeMetrics->disk_space.emplace(default_policy, default_disks[i]);
        nodeMetrics->disk_space.emplace(cold_policy, cold_disks[i]);
        container.emplace(node, nodeMetrics);
    }

    DiskStrategy strategy;

    /// Case1: no node can fulfill the request
    {
        PlacementStrategy::PlacementRequest non_request{20, "non-exists"};
        auto nodes = strategy.qualifiedNodes(container, non_request);
        EXPECT_EQ(nodes.size(), 0);
    }

    /// Case2: nodes are not enough for the request
    {
        PlacementStrategy::PlacementRequest big_request{1000, "non-exists"};
        EXPECT_EQ(strategy.qualifiedNodes(container, big_request).size(), 0);
    }

    std::uniform_int_distribution<size_t> required_number(0, 100);
    /// Case3: get nodes for default policy
    {
        size_t required = required_number(g);
        PlacementStrategy::PlacementRequest default_request{required, default_policy};
        auto nodes = strategy.qualifiedNodes(container, default_request);
        EXPECT_EQ(nodes.size(), required);
        for (int i = 0; i < required; i++)
        {
            EXPECT_EQ(nodes[i]->disk_space.find(default_policy)->second, 100 - i);
        }
    }

    /// Case4: get nodes for cold policy
    {
        size_t cold_required = required_number(g);
        PlacementStrategy::PlacementRequest cold_request{cold_required, cold_policy};
        auto cold_nodes = strategy.qualifiedNodes(container, cold_request);
        EXPECT_EQ(cold_nodes.size(), cold_required);
        for (int i = 0; i < cold_required; i++)
        {
            EXPECT_EQ(cold_nodes[i]->disk_space.find(cold_policy)->second, 100 - i);
        }
    }
}
