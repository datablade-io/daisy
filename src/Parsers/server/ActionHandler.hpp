#ifndef ACTIONHANDLER_HPP
#define ACTIONHANDLER_HPP

#include <exception>
#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>

class ActionHandler
{
    public:
        ActionHandler() {}

        virtual int action(DB::ASTPtr & ast, Poco::JSON::Object::Ptr& jsonObj)
        {
            (void)ast;
            (void)jsonObj;
            return 0;
        }

        static ActionHandler* makeHandler()
        {
            return new ActionHandler;
        }

        virtual ~ActionHandler()
        {
        }
};

#endif

