#include "JSON2QueryReadBuffer.h"

#include "ReadBufferFromString.h"
#include "ReadHelpers.h"
#include "copyData.h"

#include <common/find_symbols.h>

namespace DB
{
namespace
{
    inline void skipToChar(char c, ReadBuffer & buf)
    {
        skipWhitespaceIfAny(buf);
        assertChar(c, buf);
        skipWhitespaceIfAny(buf);
    }
}

JSON2QueryReadBuffer::JSON2QueryReadBuffer(std::unique_ptr<ReadBuffer> in_, const String & table_name_)
    : in(std::move(in_)), table_name(table_name_), columns_read(false), data_in_square_brackets(false)
{
}

bool JSON2QueryReadBuffer::nextImpl()
{
    if (!columns_read)
    {
        skipToChar('{', *in);
    }

    /// Read columns at first read
    if (!columns_read && !in->eof())
    {
        String name;
        readJSONString(name, *in);
        if (name == "columns")
        {
            auto cols = readColumns(*in);

            if (cols.length() > 0)
            {
                String query = "INSERT INTO " + table_name + " " + cols + " FORMAT JSONCompactEachRow ";
                size_t query_size = query.size();
                memory.resize(query.size());
                ::memcpy(memory.data(), query.data(), query.size());
                working_buffer = Buffer(memory.data(), memory.data() + query_size);
                columns_read = true;
                return true;
            }
            else
            {
                working_buffer = Buffer(in->position(), in->position());
                return false;
            }
        }
    }

    /// Read data fields
    if (columns_read && !in->eof())
    {
        if (!data_in_square_brackets)
        {
            String name;
            readJSONString(name, *in);

            if (name == "data")
            {
                skipToChar(':', *in);
                if (*in->position() != '[')
                {
                    char err[2] = {'[', '\0'};
                    throwAtAssertionFailed(err, *in);
                }
                data_in_square_brackets = true;
            }
        }

        if (data_in_square_brackets)
        {
            working_buffer = Buffer(in->position(), in->buffer().end());
            in->position() = in->buffer().end();
            return true;
        }
    }

    return false;
}

String JSON2QueryReadBuffer::readColumns(ReadBuffer & buf)
{
    String s;
    skipToChar(':', buf);
    bool in_bracket = false;
    while (!buf.eof())
    {
        if (!in_bracket)
        {
            if (*buf.position() != '[')
            {
                char err[2] = {'[', '\0'};
                throwAtAssertionFailed(err, *in);
            }
            in_bracket = true;
        }


        char * next_pos = find_first_symbols<'}', ']'>(buf.position() + 1, buf.buffer().end());

        s.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;
        if (*next_pos == '}')
        {
            s.clear();
        }

        if (buf.hasPendingData())
        {
            if (*buf.position() == ']')
            {
                ++buf.position();
                s.append(1, ')');
            }
            s[0] = '(';
            skipToChar(',', buf);
            return s;
        }
    }
    return s;
}
}
