#pragma once

#include <Poco/JSON/Parser.h>
#include <common/types.h>

#include <map>

namespace DB
{
class SchemaValidator
{
public:
    SchemaValidator() { }
    virtual ~SchemaValidator() { }

    static void validateSchema(const std::map<String, std::map<String, String>> schema, Poco::JSON::Object::Ptr payload);
};

}
