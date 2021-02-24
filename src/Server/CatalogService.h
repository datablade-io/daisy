#pragma once

#include <boost/noncopyable.hpp>

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <any>


namespace DB
{
class Context;

class CatalogService : private boost::noncopyable
{
public:
    explicit CatalogService(Context & context_);

    /// `broadcast` broadcasts the metadata catalog on this node
    /// to CatalogService role nodes
    void broadcast();

private:
    void init();
    void doBroadcast();
    void processQuery(BlockInputStreamPtr & in);
    void processQueryWithProcessors(QueryPipeline & pipeline);
    void commit(Block && block);
    void backgroundCataloger(const String & topic, Int32 replication_factor);

private:
    /// global context
    Context & global_context;

    std::any ctx;
    DistributedWriteAheadLogPtr dwal;

    ThreadPool cataloger;

    Poco::Logger * log;
};
}
