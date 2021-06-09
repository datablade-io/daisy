#pragma once

#include <Parsers/IAST_fwd.h>
#include <common/types.h>

/// Daisy : starts. added by Daisy

namespace DB
{
std::vector<std::string> streamingTables(const ASTPtr & ast, const String & current_db, bool enforce_single_select = true);
}
