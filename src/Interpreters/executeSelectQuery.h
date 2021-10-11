#pragma once

#include <Core/Block.h>

namespace DB
{
void executeSelectQuery(
    const String & query, 
    ContextMutablePtr query_context, 
    const std::function<void(Block &&)> & callback, 
    bool internal = true);
}
