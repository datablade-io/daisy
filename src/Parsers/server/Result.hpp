#ifndef RESULT_HPP
#define RESULT_HPP

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPResponse.h>
#include "RewriterLogger.hpp"


class Result
{
    public:
        Poco::Net::HTTPResponse::HTTPStatus httpCode;
        std::vector<std::string> faildRuleList;
        std::string lastError;
        std::string sql;
        std::string srcsql;
        int successCount;
        int ruleCount;

    public:
        Result()
        {
            httpCode = Poco::Net::HTTPResponse::HTTP_OK;
            lastError = "success";
            sql = "";
            srcsql = "";
            successCount = 0;
            ruleCount = 0;
        }

        std::string jsonify()
        {
            std::ostringstream oss;
            Poco::JSON::Object json = toJsonObject();

            json.stringify(oss);
            return oss.str();
        }

        Poco::JSON::Object toJsonObject()
        {
            Poco::JSON::Object json;

            json.set(std::string("faildRuleList"), faildRuleList);
            json.set(std::string("httpCode"), int(httpCode));
            json.set(std::string("lastError"), lastError);
            json.set(std::string("sql"), sql);
            json.set(std::string("srcsql"), srcsql);
            json.set(std::string("successCount"), successCount);
            json.set(std::string("ruleCount"), ruleCount);

            return json;
        }

};

#endif

