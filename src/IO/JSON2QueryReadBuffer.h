#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <common/StringRef.h>


namespace DB
{
/** Reads from input buffer which is request body of REST Api call
 */
class JSON2QueryReadBuffer final : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit JSON2QueryReadBuffer(std::unique_ptr<ReadBuffer> in_, const String & table_name_);

private:
    String readColumns(ReadBuffer & buf);
    std::unique_ptr<ReadBuffer> in;
    const std::string table_name;
    bool columns_read;
    bool data_in_square_brackets;

    bool nextImpl() override;
};
}
