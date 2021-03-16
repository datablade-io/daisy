#pragma once

#include <common/types.h>
#include <Poco/JSON/Parser.h>

#include <map>



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
