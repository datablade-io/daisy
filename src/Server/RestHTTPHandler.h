#pragma once

#include <Core/Names.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>

#include <re2/re2.h>

namespace CurrentMetrics
{
extern const Metric HTTPConnection;
}

namespace Poco
{
class Logger;
}

namespace DB
{
class Output;
class IServer;

class RestHTTPHandler : public HTTPRequestHandler
{
public:
    RestHTTPHandler(IServer & server_, const std::string & name);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    Poco::Logger * log;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    void trySendExceptionToClient(
        const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output);

    void executeAction(
        IServer & server_,
        Poco::Logger * log_,
        Context & context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        Output & used_output,
        std::optional<CurrentThread::QueryScope> & query_scope);
};

}
