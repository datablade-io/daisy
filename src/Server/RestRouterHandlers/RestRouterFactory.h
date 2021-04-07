#pragma once

#include "IngestRestRouterHandler.h"
#include "IngestStatusHandler.h"
#include "RestRouterHandler.h"
#include "SQLAnalyzerRestRouterHandler.h"
#include "TableRestRouterHandler.h"

#include <re2/re2.h>

#include <unordered_map>

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
        auto & factory = RestRouterFactory::instance();

        factory.registerRouterHandler(
            "/dae/v1/ingest/(?P<database>\\w+)/tables/(?P<table>\\w+)(\\?mode=\\w+){0,1}",
            "POST",
            [](Context & query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<IngestRestRouterHandler>(query_context);
            });

        factory.registerRouterHandler(
            "/dae/v1/ingest/statuses/(?P<poll_id>.+)",
            "GET",
            [](Context & query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<IngestStatusHandler>(query_context);
            });

        factory.registerRouterHandler(
            "/dae/v1/ddl/(?P<database>\\w+)/tables(\\?[\\w\\-=&#]+){0,1}",
            "GET/POST",
            [](Context & query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<TableRestRouterHandler>(query_context);
            });

        factory.registerRouterHandler(
            "/dae/v1/ddl/(?P<database>\\w+)/tables/(?P<table>\\w+)(\\?[\\w\\-=&#]+){0,1}",
            "PATCH/DELETE",
            [](Context & query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<TableRestRouterHandler>(query_context);
            });

        factory.registerRouterHandler(
            "/dae/v1/sqlanalyzer",
            "POST",
            [](Context & query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<SQLAnalyzerRestRouterHandler>(query_context);
            });
    }

public:
    RestRouterHandlerPtr get(const String & url, const String & method, Context & query_context) const
    {
        for (auto & router_handler : router_handlers)
        {
            int num_captures = router_handler.regex->NumberOfCapturingGroups() + 1;

            /// Match request method
            if (router_handler.method.find(method) != String::npos)
            {
                /// Captures param value
                re2::StringPiece matches[num_captures];

                /// Match request url
                if (router_handler.regex->Match(url, 0, url.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
                {
                    auto handler = router_handler.handler(query_context);

                    /// Get param name
                    for (const auto & [capturing_name, capturing_index] : router_handler.regex->NamedCapturingGroups())
                    {
                        const auto & capturing_value = matches[capturing_index];
                        if (capturing_value.data())
                        {
                            /// Put path parameters into handler map<string name, string value>
                            handler->setPathParameter(capturing_name, String(capturing_value.data(), capturing_value.size()));
                        }
                    }

                    return handler;
                }
            }
        }

        return nullptr;
    }

    void registerRouterHandler(const String & route, const String & method, std::function<RestRouterHandlerPtr(Context &)> func)
    {
        auto regex = compileRegex(route);
        router_handlers.emplace_back(RouterHandler(method, regex, func));
    }

private:
    CompiledRegexPtr compileRegex(const String & expression)
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

    struct RouterHandler
    {
        String method;
        CompiledRegexPtr regex;
        std::function<RestRouterHandlerPtr(Context &)> handler;

        RouterHandler(const String & method_, const CompiledRegexPtr & regex_, std::function<RestRouterHandlerPtr(Context &)> handler_)
            : method(method_), regex(regex_), handler(handler_)
        {
        }
    };

    std::vector<RouterHandler> router_handlers;
};

}
