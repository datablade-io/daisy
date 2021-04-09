#pragma once

#include <Core/Block.h>

namespace DB
{
    void executeSelectQuery(const String & query, Context & query_context, const std::function<void(Block &&)> & callback);
}
