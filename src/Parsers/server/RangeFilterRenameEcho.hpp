#ifndef RangeFilterRenameFieldRename_hpp
#define RangeFilterRenameFieldRename_hpp

#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>
#include "ActionHandler.hpp"
#include "RewriterLogger.hpp"

#include <iostream>

class RangeFilterRenameEcho: public ActionHandler
{
    public:
        virtual int action(DB::ASTPtr & ast, Poco::JSON::Object::Ptr& jsonObj) override
        {
            rewriterLogger("handle RangeFilterRenameEcho");
            (void)ast;
            (void)jsonObj;
            return 0;
        }
    
        static ActionHandler* makeHandler()
        {
            return new RangeFilterRenameEcho;
        }

    public:
        static std::string ruleName;
};
std::string RangeFilterRenameEcho::ruleName = "echo";

#endif

