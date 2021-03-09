//
// Created by zhanghong3 on 2021/3/3.
//

#ifndef CLICKHOUSE_SCHEMAVALIDATOR_H
#define CLICKHOUSE_SCHEMAVALIDATOR_H

#include "common/types.h"
#include <map>
#include <Poco/JSON/Parser.h>

namespace DB
{
    class SchemaValidator
    {
        public:
            SchemaValidator() { }
            virtual ~SchemaValidator() { }
            static void  validateSchema(std::map<String, std::map<String, String>> schema, Poco::JSON::Object::Ptr payload);
    };

}


#endif //CLICKHOUSE_SCHEMAVALIDATOR_H
