#pragma once

#include "IServer.h"

#include <Server/HTTP/HTMLForm.h>
#include <Poco/Net/HTTPRequestHandler.h>

namespace Poco { class Logger; }

namespace DB
{

class SQLAnalyzeHTTPHandler : public Poco::Net::HTTPRequestHandler
{

public:
    SQLAnalyzeHTTPHandler(IServer & server_, const std::string & name);
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    void processQuery(
        Context & context,
        Poco::Net::HTTPServerRequest & request,
        HTMLForm & params,
        Poco::Net::HTTPServerResponse & response);

private:
    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    std::string server_display_name;

    IServer & server;
    Poco::Logger * log;
};

}
