#pragma once

#include <unordered_map>
#include <vector>
#include <boost/noncopyable.hpp>
#include <common/types.h>

namespace DB
{
using DiskSpace = std::unordered_map<String, UInt64>; /// (policy name, free disk space)
struct HostState
{
    /// `host` is network reachable like hostname, FQDN or IP
    String host;
    /// `host_identity` can be unique uuid
    String host_identity;
    /// `(policy name, free disk space)`
    DiskSpace disk_space;

    explicit HostState(const String & host_) : host(host_) { }
};
using HostStatePtr = std::shared_ptr<HostState>;
using StateContainer = std::unordered_map<String, HostStatePtr>;

class PlacementStrategy : private boost::noncopyable
{
public:
    struct PlacementQuery
    {
        size_t required;
        String storage_policy;
    };

    PlacementStrategy() = default;
    virtual ~PlacementStrategy() = default;
    virtual std::vector<String> qualifiedHosts(const StateContainer & host_states, const PlacementQuery & query) = 0;
};

class DiskStrategy final : public PlacementStrategy
{
public:
    virtual std::vector<String> qualifiedHosts(const StateContainer & host_states, const PlacementQuery & query) override;
};

}
