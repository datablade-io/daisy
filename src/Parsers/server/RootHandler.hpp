#ifndef ROOTHANDLER_HPP
#define ROOTHANDLER_HPP

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include "ParserResponse.hpp"

#include <iostream>

// for uri "/"
class RootHandler : public Poco::Net::HTTPRequestHandler
{
    public:
        virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            std::ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            out << presp.stringify();
        }

        static Poco::Net::HTTPRequestHandler* getHandler()
        {
            return new RootHandler;
        }
};

#endif

