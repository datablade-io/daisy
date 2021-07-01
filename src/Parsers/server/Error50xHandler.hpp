#ifndef ERROR50XHANDLER_HPP
#define ERROR50XHANDLER_HPP

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include "ParserResponse.hpp"

#include <iostream>

// for 500
class Error50xHandler : public Poco::Net::HTTPRequestHandler
{
    public:
        virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            resp.setContentType("application/json;charset=utf-8");

            std::ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            presp.setValue(std::string("code"), std::string("500"));
            presp.setValue(std::string("msg"), std::string("internal server error"));
            out << presp.stringify();
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new Error50xHandler;
        }
};

#endif

