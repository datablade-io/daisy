#pragma once

#include <Poco/JSON/Parser.h>
#include <common/types.h>

#include <map>

namespace DB
{
void validateSchema(const std::map<String, std::map<String, String>> schema, Poco::JSON::Object::Ptr payload);

}
