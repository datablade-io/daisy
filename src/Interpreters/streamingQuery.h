#pragma once

#include <Parsers/IAST_fwd.h>

/// Daisy : starts. added by Daisy

namespace DB
{
std::vector<std::string> streamingTables(const ASTPtr & ast);
}
