#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/JSON.h>
#include <Parsers/parseQuery.h>

#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <cstdlib>

using namespace std;
using namespace Poco::Net;
using namespace Poco::Util;

template <class... T>
std::vector<std::string> strList(T... args)
{
    std::vector<std::string> vec;
    for(auto x : {args...})
    {
        vec.push_back(std::string(x));
    }

    return vec;
}

class ParserResponse
{
    private:
        Poco::JSON::Object json;

    public:

        ParserResponse(const std::string& uri)
        {
            init();
            setValue(std::string("uri"), uri);
        }

        ParserResponse()
        {
            init();
        }

        void init()
        {
            setValue(std::string("code"), std::string("200"));
            setValue(std::string("msg"), std::string("success"));
            Poco::JSON::Object subjson;
            json.set(std::string("value"), subjson);
        }

        ParserResponse& setValue(const std::string& key, const std::string& value)
        {
            json.set(key, value);

            return *this;
        }

        std::string stringify()
        {
            std::ostringstream oss;
            oss.exceptions(std::ios::failbit);
            json.stringify(oss);

            return oss.str();
        }
};

//for 404
class Error404Handler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_NOT_FOUND);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            out << presp.setValue(std::string("code"), std::string("404")).setValue(std::string("msg"), std::string("not found")).stringify();
        }

        static HTTPRequestHandler* getHandler()
        {
            return new Error404Handler;
        }
};

// for 500
class Error50xHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            presp.setValue(std::string("code"), std::string("500"));
            presp.setValue(std::string("msg"), std::string("internal server error"));
            out << presp.stringify();
        }

        static HTTPRequestHandler* getHandler()
        {
            return new Error50xHandler;
        }
};

// for uri "/"
class RootHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            out << presp.stringify();
        }

        static HTTPRequestHandler* getHandler()
        {
            return new RootHandler;
        }
};

// for uri "/ping"
class PingHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp(req.getURI());
            out << presp.stringify();
        }

        static HTTPRequestHandler* getHandler()
        {
            return new PingHandler;
        }
};


class Route
{
    private:
        std::string __uri;
        decltype(RootHandler::getHandler)* __handlerFactory;
        std::set<std::string> __methodSet;

    public:
        Route(const std::string& uri, decltype(RootHandler::getHandler)* handlerFactory, std::vector<std::string> &vec)
        {
            __uri = uri;
            __handlerFactory = handlerFactory;
            setMethodSet(vec);
        }

        std::string getURI()
        {
            return __uri;
        }

        HTTPRequestHandler* getHandler()
        {
            return __handlerFactory();
        }

        int setMethodSet(std::vector<std::string> vec)
        {
            __methodSet.clear();
            for(auto it = vec.begin(); it != vec.end(); it++)
            {
                __methodSet.insert(this -> toupper(*it));
            }

            return 0;
        }

        bool containMethod(const std::string& method)
        {
            return __methodSet.find(this -> toupper(method)) != __methodSet.end();
        }

        static std::string toupper(const std::string str)
        {
            std::string s = std::string(str);
            transform(s.begin(), s.end(), s.begin(), ::toupper);
            return s;
        }
};

class ParserRouter
{
    private:
        std::map<std::string, Route*> __routerMap;

    public:
        int registerRouter(const std::string &uri, decltype(RootHandler::getHandler)*  handlerFactory, std::vector<std::string> vec)
        {
            auto r = new Route(uri, handlerFactory, vec);
            __routerMap.insert(std::make_pair(uri, r));

            return 0;
        }

        HTTPRequestHandler* findHandler(const std::string &uri, const std::string& method)
        {
            try
            {
                std::cout << "uri : " << uri << std::endl;
                auto pair = __routerMap.find(uri);
                if(__routerMap.end() == pair)
                {
                    // no such uri
                    return new Error404Handler;
                }
                Route* r = pair -> second;
                if(nullptr == r)
                {
                    //something wrond
                    return new Error50xHandler;
                }
                if(!r -> containMethod(method))
                {
                    //method not matched
                    return new Error404Handler;
                }
                std::cout << "find uri : " << r -> getURI() << std::endl;
                auto handler = r -> getHandler();
                if(nullptr == handler)
                {
                    //something wrond
                    return new Error50xHandler;
                }
                return handler;
            }
            catch (...)
            {
                std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
                return new Error50xHandler;
            }
        }
};

ParserRouter router;

class ParserRequestHandlerFactory : public HTTPRequestHandlerFactory
{
    public:
        virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) override
        {
            return router.findHandler(req.getURI(), req.getMethod());
        }
};

class ParserServerApp :public ServerApplication
{
    protected:
        int main(const vector<string> &) override
        {
            HTTPServer s(new ParserRequestHandlerFactory, ServerSocket(8080), new HTTPServerParams);

            s.start();
            cout << endl << "Server started" << endl;

            waitForTerminationRequest();  // wait for CTRL-C or kill

            cout << endl << "Shutting down..." << endl;

            s.stop();

            return Application::EXIT_OK;
        }
};

void registerRouter()
{
    // TODO add url and handler
    router.registerRouter("/", RootHandler::getHandler, strList("GET", "POST"));
    router.registerRouter("/ping", PingHandler::getHandler, strList("GET", "POST"));
}

int main(int argc, char **argv)
{
    std::cout << "argc = " << argc << std::endl;
    if(argc > 1 && std::string("-d") == std::string(argv[1]))
    {
        argv++;
        argc--;
        std::cout << "daemon model" << std::endl;
        daemon(0, 0);
    }
    registerRouter();
    ParserServerApp app;

    return app.run(argc, argv);
}

