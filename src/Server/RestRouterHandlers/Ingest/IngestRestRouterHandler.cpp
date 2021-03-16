#include "IngestRestRouterHandler.h"

namespace DB
{
void IngestRestRouterHandler::parseURL(const Poco::Path & path)
{
    database_name = path[DATABASE_DEPTH_INDEX - 1];
    table_name = path[TABLE_DEPTH_INDEX - 1];
}

}
