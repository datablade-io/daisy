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
#include "ParserResponse.hpp"

#include <iostream>

// for uri "/"
class RangeFilterRename : public Poco::Net::HTTPRequestHandler
{
    public:
        virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp) override
        {
            (void)req;

            ParserResponse presp(req.getURI());
            //std::cout << "show HTMLForm:" << std::endl;
            std::string sql;
            Poco::Net::HTMLForm form(req, req.stream());
            for(const auto &it : form)
            {
                //get first sql rewrite resuest
                sql = it.first;
                break;
            }
            presp.setValue(std::string("sql"), echoSql(sql));
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

        std::string echoSql(const std::string& sql)
        {
            Poco::JSON::Parser jsonParser;
            Poco::Dynamic::Var result = jsonParser.parse(sql);
            Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
            Poco::Dynamic::Var sqlVar = object->get("sql");
            std::string sqlStr = sqlVar.toString();

            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);
            try
            {
                DB::ParserQueryWithOutput sqlParser(sqlStr.data() + sqlStr.size());
                DB::ASTPtr ast = DB::parseQuery(sqlParser, sqlStr.data(), sqlStr.data() + sqlStr.size(), "", 0, 0);

                DB::WriteBufferFromOStream out(oss, 4096);
                DB::formatAST(*ast, out, false, true);
            }
            catch (...)
            {
                oss << DB::getCurrentExceptionMessage(true) << "\n";
            }
            return oss.str();
        }
};

#endif

