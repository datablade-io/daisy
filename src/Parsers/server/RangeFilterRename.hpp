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
#include "RangeFilterRenameFieldRename.hpp"
#include "Rule.hpp"
#include "Result.hpp"
#include "RewriterLogger.hpp"

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
            resp.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            Result result;
            try
            {
                action(data, result);
                resp.setStatus(result.httpCode);
            }
            catch(...)
            {
                rewriterLogger("do action faild");
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }

            try
            {
                presp.setValue(std::string("code"), int(resp.getStatus()));
                presp.setValue(std::string("msg"), result.lastError);
                presp.setValue(std::string("value"), result.toJsonObject());
                std::ostream & out = resp.send();
                out << presp.stringify();
            }
            catch(...)
            {
                rewriterLogger("set value faild");
                std::ostringstream oss;
                oss.exceptions(std::ios::failbit);
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                rewriterLogger(" : " + oss.str());
            }
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new RangeFilterRename;
        }

        int action(const std::string& json, Result &result)
        {
            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);

            Poco::JSON::Object::Ptr object;
            std::string sqlStr;
            Poco::JSON::Array::Ptr ruleList;
            try
            {
                Poco::JSON::Parser jsonParser;
                object = jsonParser.parse(json).extract<Poco::JSON::Object::Ptr>();

                sqlStr = object->get("sql").toString();
                result.srcsql = sqlStr;
                ruleList = object->get("rules").extract<Poco::JSON::Array::Ptr>();
            }
            catch(...)
            {
                result.httpCode = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                result.lastError = oss.str();
                return -1;
            }

            DB::ASTPtr ast;
            try
            {
                DB::ParserQueryWithOutput sqlParser(sqlStr.data() + sqlStr.size());
                ast = DB::parseQuery(sqlParser, sqlStr.data(), sqlStr.data() + sqlStr.size(), "", 0, 0);
            }
            catch (...)
            {
                rewriterLogger("parse sql faild");
                result.httpCode = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                result.lastError = oss.str();
                return -1;
            }

            try
            {
                result.ruleCount = ruleList->size();
                // visitor AST to do replace action
                for(decltype(ruleList->size()) i = 0; i < ruleList->size(); i++)
                {
                    Poco::JSON::Object::Ptr rule = ruleList->get(i).extract<Poco::JSON::Object::Ptr>();
                    std::string ruleName = rule->get("rule").toString();
                    rewriterLogger("recive rule : " + ruleName);
                    ActionHandler *handler = __ruleRouter.findHandler(ruleName);
                    if(nullptr == handler)
                    {
                        rewriterLogger("can not find rule : " + ruleName);
                        result.faildRuleList.push_back(ruleName);
                        result.lastError = "not found rule : " + ruleName;
                        continue;
                    }
                    try
                    {
                        handler -> action(ast, rule);
                        delete handler;
                        result.successCount += 1;
                    }
                    catch (...)
                    {
                        result.faildRuleList.push_back(ruleName);
                        result.lastError = "do action faild : " + ruleName;
                    }
                }

            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                result.httpCode = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
                rewriterLogger("visit AST faild " + oss.str());
                result.lastError = oss.str();
                return -1;
            }

            try
            {
                //object->set();
                result.sql = DB::serializeAST(*ast, true);
            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                result.httpCode = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
                rewriterLogger("serializeAST faild " + oss.str());
                result.lastError =  oss.str();
                return -1;
            }

            return 0;
        }

        static void registerRules()
        {
            __ruleRouter.registerRule(RangeFilterRenameFieldRename::ruleName, RangeFilterRenameFieldRename::makeHandler);
            __ruleRouter.registerRule(RangeFilterRenameEcho::ruleName, RangeFilterRenameEcho::makeHandler);
        }
};
RuleRouter RangeFilterRename::__ruleRouter;

#endif

