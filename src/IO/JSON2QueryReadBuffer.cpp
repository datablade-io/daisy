#include <IO/JSON2QueryReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <common/find_symbols.h>

namespace DB
{
JSON2QueryReadBuffer::JSON2QueryReadBuffer(std::unique_ptr<ReadBuffer> in_, const std::string table_name_)
    : in(std::move(in_))
    , table_name(table_name_)
    , columns_read(false)
    , data_in_square_brackets(false)
{
}

static inline void skipToChar(char c, ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    assertChar(c, buf);
    skipWhitespaceIfAny(buf);
}

bool JSON2QueryReadBuffer::nextImpl()
{
    if(!columns_read)
    {
        skipToChar('{', *in);
    }

    /// read columns at first read
    if (!columns_read && !in->eof())
    {
        String name;
        readJSONString(name, *in);
        if (name == "columns")
        {
            String cols;
            readColumns(cols, *in);

            if(cols.length()>0)
            {
                String query = "INSERT INTO " + table_name + " " + cols + " FORMAT JSONCompactEachRow ";
                size_t query_size = query.size();
                memory.resize(query.size());
                ::memcpy(memory.data(), query.data(), query.size());
                working_buffer = Buffer(memory.data(), memory.data()+query_size);
                columns_read = true;
                return true;
            } else {
                working_buffer = Buffer(in->position(), in->position());
                return false;
            }
        }
    }

    /// read data fields
    if (columns_read && !in->eof())
    {
        if (!data_in_square_brackets) {
            String name;
            readJSONString(name, *in);

            if (name == "data")
            {
                skipToChar(':', *in);
                if(*in->position() != '[')
                {
                    char err[2] = {'[', '\0'};
                    throwAtAssertionFailed(err, *in);
                }
                data_in_square_brackets = true;
            }
        }

        if(data_in_square_brackets)
        {
            working_buffer = Buffer(in->position(), in->buffer().end());
            in->position() = in->buffer().end();
            return true;
        }
    }

    return false;
}

void JSON2QueryReadBuffer::readColumns(String & s, ReadBuffer & buf)
{
    s.clear();
    skipToChar(':', buf);
    bool in_bracket = false;
    while(!buf.eof())
    {
        if(!in_bracket) {
            if(*buf.position() != '[') {
                char err[2] = {'[', '\0'};
                throwAtAssertionFailed(err, *in);
            }
            in_bracket = true;
        }

        if (in_bracket)
        {
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
                return;
            }
        }
    }
}
}
