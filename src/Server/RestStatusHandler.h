#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/JSON/Parser.h>

#include <unordered_map>

namespace Poco
{
class Logger;
}

namespace DB
{
class IServer;
class RestStatusHandler : public HTTPRequestHandler
{
private:
    using Strings = std::vector<String>;
    using UriHandlerMap = std::unordered_map<String, std::function<String()>>;

    IServer & server;
    Poco::Logger * log;

    ContextPtr request_context;
    UriHandlerMap uri_funcs;

    String getHealth() { return "{\"status\":\"UP\"}"; }

    String getInfo();

    void registerFuncs(const String & uri, std::function<String()> callback);
    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;
    void buildInfoFromBlock(const Block & block, Poco::JSON::Object & resp) const;

public:
    RestStatusHandler(IServer & server, const String & name);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
