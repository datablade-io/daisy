#pragma once

#include <boost/noncopyable.hpp>

#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/QueryPipeline.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <any>


namespace DB
{
class Context;

class DDLService : private boost::noncopyable
{
public:
    explicit DDLService(Context & context_);
    ~DDLService();

private:
    void init();

    void createTable(const Block & bock);
    void deleteTable(const Block & bock);
    void alterTable(const Block & bock);
    void processDDL(const IDistributedWriteAheadLog::RecordPtrs & records);

    bool validateSchema(const Block & block, const std::vector<String> & col_names);

    void backgroundDDL();

private:
    /// global context
    Context & global_context;

    std::any ddl_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> ddl;

    Poco::Logger * log;
};
}
