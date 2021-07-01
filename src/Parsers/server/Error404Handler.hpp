#ifndef ERROR404HANDLER_HPP
#define ERROR404HANDLER_HPP

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include "ParserResponse.hpp"

#include <iostream>

//for 404
class Error404Handler : public Poco::Net::HTTPRequestHandler
{
    public:
        virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
            resp.setContentType("application/json;charset=utf-8");

            std::ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            out << presp.setValue(std::string("code"), std::string("404")).setValue(std::string("msg"), std::string("not found")).stringify();
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new Error404Handler;
        }
};

#endif

