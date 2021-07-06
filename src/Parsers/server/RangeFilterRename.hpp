#ifndef RANGEFILTERRENAME_HPP
#define RANGEFILTERRENAME_HPP

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromOStream.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/JSON.h>
#include <Poco/Dynamic/Var.h>
#include "ParserResponse.hpp"
#include "RangeFilterRenameEcho.hpp"
#include "Rule.hpp"

#include <iostream>

// for uri "/"
class RangeFilterRename : public Poco::Net::HTTPRequestHandler
{
    private:
        static RuleRouter __ruleRouter;

    public:
        virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp) override
        {
            (void)req;

            ParserResponse presp(req.getURI());
            //std::cout << "show HTMLForm:" << std::endl;
            std::string data;
            Poco::Net::HTMLForm form(req, req.stream());
            for(const auto &it : form)
            {
                //get first sql rewrite resuest
                data = it.first;
                break;
            }
            std::string value = action(data);
            presp.setValue(std::string("value"), value);
            //std::string content = req.read(req.stream()sql);
            //std::cout << content;
            resp.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            std::ostream & out = resp.send();
            out << presp.stringify();
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new RangeFilterRename;
        }

        std::string echoSql(const std::string& json)
        {
            Poco::JSON::Parser jsonParser;
            Poco::Dynamic::Var result = jsonParser.parse(json);
            Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
            Poco::Dynamic::Var sqlVar = object->get("sql");
            std::string sqlStr = sqlVar.toString();

            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);
            try
            {
                DB::ParserQueryWithOutput sqlParser(sqlStr.data() + sqlStr.size());
                DB::ASTPtr ast = DB::parseQuery(sqlParser, sqlStr.data(), sqlStr.data() + sqlStr.size(), "", 0, 0);

                return DB::serializeAST(*ast, true);
            }
            catch (...)
            {
                //oss << DB::getCurrentExceptionMessage(true) << "\n";
            }
            return oss.str();
        }

        std::string action(const std::string& json)
        {
            Poco::JSON::Parser jsonParser;
            Poco::JSON::Object::Ptr object = jsonParser.parse(json).extract<Poco::JSON::Object::Ptr>();

            std::string sqlStr = object->get("sql").toString();
            Poco::JSON::Array::Ptr ruleList = object->get("rules").extract<Poco::JSON::Array::Ptr>();

            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);
            try
            {
                DB::ParserQueryWithOutput sqlParser(sqlStr.data() + sqlStr.size());
                DB::ASTPtr ast = DB::parseQuery(sqlParser, sqlStr.data(), sqlStr.data() + sqlStr.size(), "", 0, 0);

                // visitor AST to do replace action
                for(decltype(ruleList->size()) i = 0; i < ruleList->size(); i++)
                {
                    Poco::JSON::Object::Ptr rule = ruleList->get(i).extract<Poco::JSON::Object::Ptr>();
                    std::string ruleName = rule->get("rule").toString();
                    ActionHandler *handler = RangeFilterRename::__ruleRouter.findHandler(ruleName);
                    handler -> action(*ast, rule);
                }
                
                //object->set();
                return DB::serializeAST(*ast, true);
            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
            }

            return oss.str();
        }

        //std::string fieldRename(DB::ASTPtr ast)
        //{
        //}

        //std::string fieldAdd(const std::string& json)
        //{
        //    Poco::JSON::Parser jsonParser;
        //    Poco::Dynamic::Var result = jsonParser.parse(json);
        //    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        //    std::string sqlStr = object->get("sql").toString();
        //    std::string field = object->get("field").toString();
        //    std::string from = object->get("from").toString();
        //    std::string to = object->get("to").toString();

        //    std::ostringstream oss;
        //    oss.exceptions(std::ios::failbit);
        //    try
        //    {
        //        DB::ParserQueryWithOutput sqlParser(sqlStr.data() + sqlStr.size());
        //        DB::ASTPtr ast = DB::parseQuery(sqlParser, sqlStr.data(), sqlStr.data() + sqlStr.size(), "", 0, 0);

        //        // visitor AST to do replace action
        //        

        //        object->set();
        //        return DB::serializeAST(*ast, true);
        //    }
        //    catch (...)
        //    {
        //        oss << DB::getCurrentExceptionMessage(true) << "\n";
        //    }

        //    return oss.str();
        //}

        static void registerRules()
        {
            //__ruleRouter.registerRule("fieldrename", RangeFilterRenameFieldRename::makeHandler);
            __ruleRouter.registerRule("echo", RangeFilterRenameEcho::makeHandler);
        }
};
RuleRouter RangeFilterRename::__ruleRouter;

#endif

