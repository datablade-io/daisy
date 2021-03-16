#pragma once

#include "RestRouterHandler.h"

#include <Server/RestRouterHandlers/Ingest/IngestRestRouterHandler.h>
#include <Server/RestRouterHandlers/DDL/TableRestRouterHandler.h>

#include <unordered_map>

namespace DB
{
class RestRouterFactory final
{
public:
    static RestRouterFactory & instance()
    {
        static RestRouterFactory router_factory;
        return router_factory;
    }

    static void registerRestRouterHandlers()
    {   
        auto & factory = DB::RestRouterFactory::instance();

        factory.registerRouterHandler("/dae/v1/ddl/tables", [](DB::Context &query_context) {
            return std::make_shared<DB::TableRestRouterHandler>(query_context);
        });

        factory.registerRouterHandler("/dae/v1/ingest", [](DB::Context &query_context) {
            return std::make_shared<DB::IngestRestRouterHandler>(query_context);
        });
    }

public:
    RestRouterHandlerPtr get(const String & route, Context & query_context) const
    {
        auto iter = router_handlers.find(route);
        if (likely(iter != router_handlers.end()))
        {
            return iter->second(query_context);
        }

        return nullptr;
    }

    void registerRouterHandler(const String & route, std::function<RestRouterHandlerPtr(Context &)> func)
    {
        if (router_handlers.contains(route))
        {
            throw Exception("There is already a processing program corresponding to the route : " + route, ErrorCodes::UNACCEPTABLE_URL);
        }

        router_handlers[route] = func;
    }

private:
    std::unordered_map<String, std::function<RestRouterHandlerPtr(Context &)>> router_handlers;
};

}
