#ifndef PARSERRESPONSE_HPP
#define PARSERRESPONSE_HPP

#include <Poco/JSON/Object.h>

class ParserResponse
{
    private:
        Poco::JSON::Object json;

    public:

        ParserResponse(const std::string& uri)
        {
            init();
            setValue(std::string("uri"), uri);
        }

        ParserResponse()
        {
            init();
        }

        void init()
        {
            setValue(std::string("code"), std::string("200"));
            setValue(std::string("msg"), std::string("success"));
            Poco::JSON::Object subjson;
            json.set(std::string("value"), subjson);
        }

        template <class T>
        ParserResponse& setValue(const std::string& key, const T& value)
        {
            json.set(key, value);

            return *this;
        }

        std::string stringify()
        {
            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);
            json.stringify(oss);

            return oss.str();
        }
};

#endif

