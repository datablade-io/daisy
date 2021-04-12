#include "JSONHelper.h"

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Common/PODArray.h>
#include <common/JSON.h>

namespace DB
{
bool readIntoBuffers(ReadBuffer & from, PODArray<char> & to, Buffers & buffers, String & error)
{
    WriteBufferFromVector<PODArray<char>> tmp_buf(to);
    copyData(from, tmp_buf);
    tmp_buf.finalize();

    const char * begin = to.data();
    const char * end = begin + to.size();
    const char * pre = begin;
    String name;

    JSON obj{begin, end};
    try
    {
        for (auto it = obj.begin(); it != obj.end(); ++it)
        {
            if (pre != begin)
            {
                size_t size = static_cast<size_t>(it.data() - pre);
                buffers.emplace(name, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(pre), size));
            }
            name = it.getName();
            pre = it.getValue().data();
        }

        if (pre != begin)
            buffers.emplace(name, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(pre), static_cast<size_t>(end - pre)));
    }
    catch (JSONException & e)
    {
        error = e.message();
        return false;
    }
    return true;
}
}
