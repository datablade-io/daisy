#include <Core/Block.h>
#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

namespace DB
{
Block buildBlock(
    const std::vector<std::pair<String, String>> & string_cols,
    std::vector<std::pair<String, Int32>> int32_cols,
    std::vector<std::pair<String, UInt64>> uint64_cols);

void append(Block && block, Context & context, IDistributedWriteAheadLog::OpCode opCode, const Poco::Logger * log);

}
