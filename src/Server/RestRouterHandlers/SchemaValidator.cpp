#include "SchemaValidator.h"

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
}

void validateSchema(const std::map<String, std::map<String, String>> & schema, const Poco::JSON::Object::Ptr & payload)
{
    auto iter = schema.find("required");
    if (iter != schema.end())
    {
        for (const auto & required : iter->second)
        {
            if (!payload->has(required.first))
            {
                throw Exception("Required param '" + required.first + "' is missing.", ErrorCodes::POCO_EXCEPTION);
            }

            if ((required.second == "int" && !payload->get(required.first).isInteger())
                || (required.second == "string" && !payload->get(required.first).isString())
                || (required.second == "bool" && !payload->get(required.first).isBoolean())
                || (required.second == "double" && !payload->get(required.first).isNumeric())
                || (required.second == "array" && !payload->get(required.first).isArray()))
            {
                throw Exception("Invalid type of param '" + required.first + "'", ErrorCodes::POCO_EXCEPTION);
            }
        }
    }

    iter = schema.find("optional");
    if (iter != schema.end())
    {
        for (const auto & optional : iter->second)
        {
            if (payload->has(optional.first)
                && ((optional.second == "int" && !payload->get(optional.first).isInteger())
                    || (optional.second == "string" && !payload->get(optional.first).isString())
                    || (optional.second == "bool" && !payload->get(optional.first).isBoolean())
                    || (optional.second == "double" && !payload->get(optional.first).isNumeric())))
            {
                throw Exception("Invalid type of param '" + optional.first + "'", ErrorCodes::POCO_EXCEPTION);
            }
        }
    }
}
}
