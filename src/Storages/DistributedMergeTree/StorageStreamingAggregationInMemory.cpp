#include "StorageStreamingAggregationInMemory.h"

#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

namespace ErrorCodes
{
    const extern int INCORRECT_QUERY;
}

StorageStreamingAggregationInMemory::StorageStreamingAggregationInMemory(
    const StorageID & storage_id_, const ASTPtr & select_query, ContextPtr query_context_, Poco::Logger * log_)
    : IStorage(storage_id_), query_context(query_context_), log(log_)
{
    if (auto * union_select = select_query->as<ASTSelectWithUnionQuery>())
    {
        /// init(union_select);
    }
    else
    {
        throw Exception("Not a SELECT query" + getName(), ErrorCodes::INCORRECT_QUERY);
    }
}

/*void StorageStreamingAggregationInMemory::init(ASTSelectWithUnionQuery * select)
{
}*/

void StorageStreamingAggregationInMemory::startup()
{
}

Pipe StorageStreamingAggregationInMemory::read(
    const Names & column_names,
    const StorageMetadataPtr & /* metadata_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    (void)column_names;
    (void)query_info;
    (void)context;
    (void)processed_stage;
    (void)max_block_size;
    (void)num_streams;
    (void)log;
    return {};
}
}
