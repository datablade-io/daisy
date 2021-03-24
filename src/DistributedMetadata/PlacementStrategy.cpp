#include "PlacementStrategy.h"


namespace DB
{
std::vector<String> DiskStrategy::qualifiedHosts(const StateContainer & host_states, const PlacementQuery & query)
{
    if (query.required <= 0)
    {
        return {};
    }

    using Disk = std::pair<String, UInt64>;
    std::vector<Disk> disks;
    disks.reserve(host_states.size());

    for (const auto & [host, metrics] : host_states)
    {
        auto iter = metrics->disk_space.find(query.storage_policy);
        if (iter != metrics->disk_space.end() && iter->second > 0)
        {
            disks.emplace_back(host, iter->second); //stack or heap?
        }
    }

    if (disks.size() < query.required)
    {
        return {};
    }

    std::sort(std::begin(disks), std::end(disks), [&](Disk a, Disk b) { return a.second > b.second; });
    std::vector<String> res;
    res.reserve(disks.size());
    for (size_t i = 0; i < query.required; i++)
    {
        res.push_back(disks[i].first);
    }
    return res;
}

}
