#pragma once

#include "RestRouterHandler.h"

#include <Server/RestRouterHandlers/DDL/TableRestRouterHandler.h>
#include <Server/RestRouterHandlers/Ingest/IngestRestRouterHandler.h>

#include <unordered_map>

#include <re2/re2.h>

namespace DB
{
using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

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

        factory.registerRouterHandler(
            "/dae/v1/ddl/(?P<database>\\w+)/tables/(?P<table>\\w+)", "PATCH/DELETE", [](DB::Context & query_context) {
            return std::make_shared<DB::TableRestRouterHandler>(query_context);
         });

        factory.registerRouterHandler("/dae/v1/ddl/(?P<database>\\w+)/tables", "GET/POST", [](DB::Context & query_context) {
            return std::make_shared<DB::TableRestRouterHandler>(query_context);
        });

        factory.registerRouterHandler("/dae/v1/ingest/(?:database\\w+)/tables/(?:table:\\w+)", "POST", [](DB::Context & query_context) {
            return std::make_shared<DB::IngestRestRouterHandler>(query_context);
        });

        factory.registerRouterHandler("/dae/v1/ingest/statuses/(?:poll_id.+)", "GET", [](DB::Context & query_context) {
            return std::make_shared<DB::IngestRestRouterHandler>(query_context);
        });
    }

public:
    RestRouterHandlerPtr get(const String & url, const String & method, Context & query_context) const
    {
        auto iter = router_handlers.begin();
        for (; iter != router_handlers.end(); ++iter)
        {
            CompiledRegexPtr compiled_regex = iter->first.second;
            int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

            /// Match request method
            if (iter->first.first.find(method) != String::npos)
            {
                /// captures param value
                re2::StringPiece matches[num_captures];

                /// Match request url
                if (compiled_regex->Match(url, 0, url.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
                {
                    /// Get param name
                    for (const auto & [capturing_name, capturing_index] : compiled_regex->NamedCapturingGroups())
                    {
                        const auto & capturing_value = matches[capturing_index];
                        if (capturing_value.data())
                            /// Put parameters into query_context.QueryParameter map< string name, string value>
                            query_context.setQueryParameter(capturing_name, String(capturing_value.data(), capturing_value.size()));
                    }

                    return iter->second(query_context);
                }
            }
        }

        return nullptr;
    }

    void registerRouterHandler(const String & route, const String & method, std::function<RestRouterHandlerPtr(Context &)> func)
    {
        auto regex = getCompiledRegex(route);
        router_handlers.emplace(std::make_pair(method, regex), func);
    }

private:
    CompiledRegexPtr getCompiledRegex(const String & expression)
    {
        auto compiled_regex = std::make_shared<const re2::RE2>(expression);

        if (!compiled_regex->ok())
        {
            throw Exception(
                "Cannot compile re2: " + expression + " for http handling rule, error: " + compiled_regex->error()
                    + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
                ErrorCodes::CANNOT_COMPILE_REGEXP);
        }

        return compiled_regex;
    }

    /// (method, regex(url))
    using url_regex = std::pair<String, CompiledRegexPtr>;

    /// (method, regex(url)) -> RestRouterHandler)
    std::unordered_map<url_regex, std::function<RestRouterHandlerPtr(Context &)>, boost::hash<url_regex>> router_handlers;
};

}
