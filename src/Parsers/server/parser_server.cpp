#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>

#include <iostream>
#include <string>
#include <vector>
#include <set>

using namespace std;
using namespace Poco::Net;
using namespace Poco::Util;

class ParserRequestHandler : public HTTPRequestHandler
{
    public:
        virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) override
        {
            (void)req;
            resp.setStatus(HTTPResponse::HTTP_OK);
            resp.setContentType("text/html");

            ostream & out = resp.send();

            out << "<h1>Hello World!</h1>";
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

        HTTPRequestHandler* findHandler(const std::string &uri)
        {
            auto pair = __routerMap.find(uri);
            if(__routerMap.end() == pair)
            {
                // no such uri
            }
            Route* r = pair -> second;

            return r -> getHandler();
        }
};

ParserRouter router;

class ParserRequestHandlerFactory : public HTTPRequestHandlerFactory
{
    public:
        virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) override
        {
            if (req.getURI() == "/")
            {
                std::cout << "get path '/'" << std::endl;
            }
            return new ParserRequestHandler;
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
    ParserServerApp app;

    return app.run(argc, argv);
}

