#ifndef RANGEFILTERRENAMEFIELDRENAME_HPP
#define RANGEFILTERRENAMEFIELDRENAME_HPP

#include "ActionHandler.hpp"
#include <Poco/JSON/Object.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>

class RangeFilterRenameFieldRename: public ActionHandler
{
    public:
        virtual int action(DB::ASTPtr & ast, Poco::JSON::Object::Ptr& jsonObj) override
        {
            rewriterLogger("handle " + ruleName);
            std::string rule = jsonObj->get("rule").toString();
            if(rule != ruleName)
            {
                throw "request is no " + ruleName;
            }
            std::string from = jsonObj->get("from").toString();
            std::string to = jsonObj->get("to").toString();
            visitor(ast, from, to);

            return 0;
        }
    
        static ActionHandler* makeHandler()
        {
            return new RangeFilterRenameFieldRename;
        }

        void visitor(DB::ASTPtr & ast, std::string &from, const std::string& to)
        {
            auto & list_of_selects = ast->as<DB::ASTSelectWithUnionQuery>()->list_of_selects->children;

            for (auto & select : list_of_selects)
            {
                auto * query = select->as<DB::ASTSelectQuery>();
                for (auto & column : query->select()->children) {
                    auto * id = column->as<DB::ASTIdentifier>();
                    if (id->name() == from) {
                        std::cout<< column->getAliasOrColumnName();
                        id -> setShortName(to);
                    }
                }
            }
        }

    public:
        static std::string ruleName;
};
std::string RangeFilterRenameFieldRename::ruleName = "fieldrename";

#endif

