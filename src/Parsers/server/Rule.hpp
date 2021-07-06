#ifndef RULE_HPP
#define RULE_HPP

#include "ActionHandler.hpp"

class Rule
{
    private:
        std::string __name;
        decltype(ActionHandler::makeHandler)* __handlerFactory;

    public:
        Rule(const std::string& ruleName, decltype(ActionHandler::makeHandler)* handlerFactory)
        {
            __name = ruleName;
            __handlerFactory = handlerFactory;
        }

        ActionHandler* getHandler()
        {
            return __handlerFactory();
        }
};

class RuleRouter
{
    private:
        std::map<std::string, Rule*> __routerMap;

    public:
        ActionHandler* findHandler(const std::string& ruleName)
        {
            try
            {
                auto pair = __routerMap.find(ruleName);
                if(__routerMap.end() == pair)
                {
                    // no such uri
                    return new ActionHandler;
                }
                Rule* r = pair -> second;
                if(nullptr == r)
                {
                    //something wrond
                    return new ActionHandler;
                }
                auto handler = r -> getHandler();
                if(nullptr == handler)
                {
                    //something wrond
                    return new ActionHandler;
                }
                return handler;
            }
            catch (...)
            {
                return new ActionHandler;
            }
        }

        void registerRule(const std::string& ruleName, decltype(ActionHandler::makeHandler)* handlerFactory)
        {
            Rule* r = new Rule(ruleName, handlerFactory);
            __routerMap.insert(std::make_pair(ruleName, r));
        }
};

#endif

