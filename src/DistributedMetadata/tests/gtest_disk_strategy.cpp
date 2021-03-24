#include <algorithm>
#include <random>
#include <DistributedMetadata/PlacementStrategy.h>
#include <gtest/gtest.h>


using namespace DB;

TEST(PlacementService, PlaceHosts)
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

    StateContainer container;
    String default_policy = "default";
    String cold_policy = "cold_policy";
    for (int i = 0; i < 100; i++)
    {
        String host = std::to_string(i);
        HostStatePtr hostState = std::make_shared<HostState>(host);
        hostState->disk_space.emplace(default_policy, default_disks[i]);
        hostState->disk_space.emplace(cold_policy, cold_disks[i]);
        container.emplace(host, hostState);
    }

    DiskStrategy strategy;

    /// Case1: no host can fulfill the request
    {
        PlacementStrategy::PlacementQuery non_query{20, "non-exists"};
        auto hosts = strategy.qualifiedHosts(container, non_query);
        EXPECT_EQ(hosts.size(), 0);
    }

    /// Case2: hosts are not enough for the request
    {
        PlacementStrategy::PlacementQuery big_query{1000, "non-exists"};
        EXPECT_EQ(strategy.qualifiedHosts(container, big_query).size(), 0);
    }


    std::uniform_int_distribution<size_t> required_number(0, 100);
    /// Case3: get hosts for default policy
    {
        size_t required = required_number(g);
        PlacementStrategy::PlacementQuery default_query{required, default_policy};
        auto hosts = strategy.qualifiedHosts(container, default_query);
        EXPECT_EQ(hosts.size(), required);
        for (int i = 0; i < required; i++)
        {
            auto host_name = hosts[i];
            EXPECT_EQ(container.find(host_name)->second->disk_space.find(default_policy)->second, 100 - i);
        }
    }

    /// Case4: get hosts for cold policy
    {
        size_t cold_required = required_number(g);
        PlacementStrategy::PlacementQuery cold_query{cold_required, cold_policy};
        auto cold_hosts = strategy.qualifiedHosts(container, cold_query);
        EXPECT_EQ(cold_hosts.size(), cold_required);
        for (int i = 0; i < cold_required; i++)
        {
            auto host_name = cold_hosts[i];
            EXPECT_EQ(container.find(host_name)->second->disk_space.find(cold_policy)->second, 100 - i);
        }
    }
}
