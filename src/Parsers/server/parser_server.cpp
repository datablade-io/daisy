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
class error404Handler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_NOT_FOUND);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp;
            out << presp.setValue(std::string("code"), std::string("404")).setValue(std::string("msg"), std::string("not found")).stringify();
        }
};

// for 500
class error50xHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp;
            out << presp.setValue("code", "500").setValue("msg", "internal server error").stringify();
        }
};

// for uri "/"
class rootHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp;
            presp.setValue(std::string("code"), std::string("200")).setValue(std::string("msg"), std::string("success"));
            presp.setValue(std::string("value"), std::string(""));
            presp.setValue(std::string("uri"), std::string("/"));
            out << presp.stringify();
        }
};

// for uri "/ping"
class pingHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_OK);
            resp.setContentType("application/json;charset=utf-8");

            ostream & out = resp.send();

            ParserResponse presp;
            presp.setValue(std::string("code"), std::string("200"));
            presp.setValue(std::string("msg"), std::string("success"));
            presp.setValue(std::string("uri"), std::string("/ping"));
            out << presp.stringify();
        }
};


class Route
{
    private:
        std::string __uri;
        HTTPRequestHandler* __handler;
        std::set<std::string> __methodSet;

    public:
        Route(const std::string& uri, HTTPRequestHandler* handler, std::vector<std::string> &vec)
        {
            __uri = uri;
            __handler = handler;
            setMethodSet(vec);
        }

        std::string getURI()
        {
            return __uri;
        }

        HTTPRequestHandler* getHandler()
        {
            return __handler;
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
        int registerRouter(const std::string &uri, HTTPRequestHandler *handler, std::vector<std::string> vec)
        {
            auto r = new Route(uri, handler, vec);
            __routerMap.insert(std::make_pair(uri, r));

            return 0;
        }

        HTTPRequestHandler* findHandler(const std::string &uri, const std::string& method)
        {
            std::cout << "uri : " << uri << std::endl;
            auto pair = __routerMap.find(uri);
            if(__routerMap.end() == pair)
            {
                // no such uri
                return new error404Handler;
            }
            Route* r = pair -> second;
            if(nullptr == r)
            {
                //something wrond
                return new error50xHandler;
            }
            std::cout << "find uri : " << r -> getURI() << std::endl;
            if(nullptr == r -> getHandler())
            {
                //something wrond
                return new error50xHandler;
            }
            if(!r -> containMethod(method))
            {
                //method not matched
                return new error404Handler;
            }

            return r -> getHandler();
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
    router.registerRouter("/", new rootHandler, strList("GET", "POST"));
    router.registerRouter("/ping", new pingHandler, strList("GET", "POST"));
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

