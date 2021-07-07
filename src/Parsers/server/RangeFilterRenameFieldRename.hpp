#ifndef RANGEFILTERRENAMEFIELDRENAME_HPP
#define RANGEFILTERRENAMEFIELDRENAME_HPP

#include "ActionHandler.hpp"
#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>

class RangeFilterRenameFieldRename: public ActionHandler
{
    public:
        virtual int action(const DB::IAST & ast, Poco::JSON::Object::Ptr& jsonObj) override
        {
            rewriterLogger("handle " + ruleName);
            std::string rule = jsonObj->get("rule").toString();
            if(rule != ruleName)
            {
                throw "request is no " + ruleName;
            }
            std::string from = jsonObj->get("from").toString();
            std::string to = jsonObj->get("to").toString();
            (void)ast;


            return 0;
        }
    
        static ActionHandler* makeHandler()
        {
            return new RangeFilterRenameFieldRename;
        }

    public:
        static std::string ruleName;
};
std::string RangeFilterRenameFieldRename::ruleName = "fieldrename";

#endif

