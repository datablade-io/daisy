#pragma once

#include <common/StringRef.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Reads from input buffer which is request body of REST Api call
 */
class JSON2QueryReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit JSON2QueryReadBuffer(
        std::unique_ptr<ReadBuffer> in_, const std::string table_name_);

private:
    void readColumns(String &s, ReadBuffer & buf);
    std::unique_ptr<ReadBuffer> in;
    const std::string table_name;
    bool columns_read;
    bool data_in_square_brackets;

    bool nextImpl() override;
};
}
