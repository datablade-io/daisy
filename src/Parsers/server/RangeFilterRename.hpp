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

            std::string value ;
            try
            {
                value = action(data, resp);
                presp.setValue(std::string("code"), int(resp.getStatus()));
            }
            catch(...)
            {
                rewriterLogger("do action faild");
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }

            if(Poco::Net::HTTPResponse::HTTP_OK == resp.getStatus())
            {
                rewriterLogger("HTTPResponse::HTTP_OK");
                presp.setValue(std::string("value"), value);
            }
            else
            {
                rewriterLogger("error happend in action");
                presp.setValue(std::string("msg"), value);
            }

            std::ostream & out = resp.send();
            out << presp.stringify();
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new RangeFilterRename;
        }

        std::string action(const std::string& json, Poco::Net::HTTPServerResponse &resp)
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
                ruleList = object->get("rules").extract<Poco::JSON::Array::Ptr>();
            }
            catch(...)
            {
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                return oss.str();
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
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                return oss.str();
            }

            try
            {
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
                    }
                    else
                    {
                        handler -> action(*ast, rule);
                        delete handler;
                    }
                }

            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                rewriterLogger("visit AST faild " + oss.str());
                return oss.str();
            }

            try
            {
                //object->set();
                std::string value = DB::serializeAST(*ast, true);
                return value;
            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
                resp.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                rewriterLogger("serializeAST faild " + oss.str());
                return oss.str();
            }

            return "";
        }

        static void registerRules()
        {
            __ruleRouter.registerRule(RangeFilterRenameFieldRename::ruleName, RangeFilterRenameFieldRename::makeHandler);
            __ruleRouter.registerRule(RangeFilterRenameEcho::ruleName, RangeFilterRenameEcho::makeHandler);
        }
};
RuleRouter RangeFilterRename::__ruleRouter;

#endif

