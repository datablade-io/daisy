
#pragma once

#include "RestRouterHandler.h"

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
            throw Exception("Cannot find the handler corresponding to the route : " + route, ErrorCodes::UNACCEPTABLE_URL);
        }

        router_handlers[route] = func;
    }

private:

    std::unordered_map<String, std::function<RestRouterHandlerPtr(Context &)>> router_handlers;
};

}
