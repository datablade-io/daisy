#pragma once

#include <IO/ReadBufferFromMemory.h>
#include <Common/PODArray.h>
#include <common/SimpleJSON.h>

#include <unordered_map>

namespace DB
{
using JSONReadBuffers = std::unordered_map<std::string, std::shared_ptr<ReadBuffer>>;

bool readIntoBuffers(ReadBuffer & from, PODArray<char> & to, JSONReadBuffers & buffers, String & error);
}
