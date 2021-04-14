#pragma once

#include "RestRouterHandler.h"

#include <DataStreams/IBlockStream_fwd.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Processors/QueryPipeline.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <boost/functional/hash.hpp>

namespace DB
{
namespace
{
std::map<String, std::map<String, String> > CREATE_SCHEMA = {
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

std::map<String, std::map<String, String> > COLUMN_SCHEMA = {
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

std::map<String, std::map<String, String> > UPDATE_SCHEMA = {
        {"required",{
                    }
        },
        {"optional", {
                            {"ttl_expression", "string"}
                    }
        }
};

std::map<String, String> GRANULARITY_FUNC_MAPPING= {
        {"M", "toYYYYMM(`_time`)"},
        {"D", "toYYYYMMDD(`_time`)"},
        {"H", "toStartOfHour(`_time`)"},
        {"m", "toStartOfMinute(`_time`)"}
};
}

class TableRestRouterHandler : public RestRouterHandler
{
public:
    explicit TableRestRouterHandler(Context & query_context_, const String & router_name = "Table")
        : RestRouterHandler(query_context_, router_name)
        , create_schema(CREATE_SCHEMA)
        , column_schema(COLUMN_SCHEMA)
        , update_schema(UPDATE_SCHEMA)
        , granularity_func_mapping(GRANULARITY_FUNC_MAPPING)
    {
    }
    ~TableRestRouterHandler() override { }

private:
    virtual bool validateGet(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    virtual bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    virtual bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const;
    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;

    String getTimeColumn(const Poco::JSON::Object::Ptr & payload) const;

    virtual String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const;
    String getOrderbyExpr(const Poco::JSON::Object::Ptr & payload, const String & time_column) const;

protected:
    std::map<String, std::map<String, String> > & create_schema;
    std::map<String, std::map<String, String> > & column_schema;
    std::map<String, std::map<String, String> > & update_schema;
    std::map<String, String> & granularity_func_mapping;

    String buildResponse() const;
    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;
    String getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity) const;
    String processQuery(const String & query) const;

    virtual String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    virtual String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
};

}
