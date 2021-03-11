#ifndef CLICKHOUSE_SCHEMAVALIDATOR_H
#define CLICKHOUSE_SCHEMAVALIDATOR_H

#include <map>
#include <Poco/JSON/Parser.h>
#include "common/types.h"

namespace DB
{
class SchemaValidator
{
public:
    SchemaValidator() { }
    virtual ~SchemaValidator() { }
    static void validateSchema(std::map<String, std::map<String, String>> schema, Poco::JSON::Object::Ptr payload);
};

}


#endif //CLICKHOUSE_SCHEMAVALIDATOR_H
