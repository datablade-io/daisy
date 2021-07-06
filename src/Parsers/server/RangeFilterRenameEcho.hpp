#ifndef RangeFilterRenameFieldRename_hpp
#define RangeFilterRenameFieldRename_hpp

#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>
#include "ActionHandler.hpp"

#include <iostream>

class RangeFilterRenameEcho: public ActionHandler
{
    public:
        virtual int action(const DB::IAST & ast, Poco::JSON::Object::Ptr& jsonObj) override
        {
            std::cout << "handle RangeFilterRenameEcho" << std::endl;
            (void)ast;
            (void)jsonObj;
            return 0;
        }
    
        static ActionHandler* makeHandler()
        {
            return new RangeFilterRenameEcho;
        }
};

#endif

