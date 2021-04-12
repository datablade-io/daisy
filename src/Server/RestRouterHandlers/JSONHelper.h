#pragma once

#include <IO/ReadBufferFromMemory.h>
#include <Common/PODArray.h>
#include <common/JSON.h>

#include <unordered_map>

namespace DB
{
using Buffers = std::unordered_map<std::string, std::shared_ptr<ReadBuffer>>;

bool readIntoBuffers(ReadBuffer & from, PODArray<char> & to, Buffers & buffers, String & error);
}
