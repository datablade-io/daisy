#pragma once

#include "common/types.h"

#include <algorithm>
#include <map>

#include <Core/Names.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>

#include <re2/re2.h>


#define CATEGORY_DEPTH 3
#define DATABASES_DEPTH 4
#define TABLES_DEPTH 5
#define COLUMN_DEPTH 6
#define RAWSTORE_DEPTH 4

namespace DB
{
class IServer;
class WriteBufferFromHTTPServerResponse;
class Output
{
    /* Raw data
     * ↓
     * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
     * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
     * CompressedWriteBuffer out_maybe_compressed (optional)
     * ↓
     * WriteBufferFromHTTPServerResponse out
     */
public:
    std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
    /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
    std::shared_ptr<WriteBuffer> out_maybe_compressed;
    /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
    std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

    inline bool hasDelayed() const
    {
        return out_maybe_delayed_and_compressed != out_maybe_compressed;
    }
};

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_COMPILE_REGEXP;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;

    extern const int BAD_REQUEST_PARAMETER;
    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
}

class IRestAction
{

public:
    IRestAction(){}
    virtual ~IRestAction(){}

    virtual void execute(
            IServer & server,
            Poco::Logger * log,
            Context & context,
            HTTPServerRequest & request,
            HTMLForm & params,
            HTTPServerResponse & response,
            Output & used_output,
            std::optional<CurrentThread::QueryScope> & query_scope,
            const Poco::Path & path) = 0;

    /// Also initializes 'used_output'.
    void virtual executeByQuery(
            IServer & server,
            Poco::Logger * log,
            Context & context,
            HTTPServerRequest & request,
            HTMLForm & params,
            HTTPServerResponse & response,
            Output & used_output,
            std::optional<CurrentThread::QueryScope> & query_scope,
            String & query);

private:
    static void pushDelayedResults(Output & used_output);

};


class RestActionFactory
{
public:
    template<typename T>
    struct ActionRegister
    {
        ActionRegister(const String& key)
        {
            RestActionFactory::get().dyn_acthion_map.emplace(key, [] { return new T(); });
        }
    };

    static IRestAction* produce(const String& key)
    {
        if (dyn_acthion_map.find(key) == dyn_acthion_map.end())
            throw Exception("Invalid path name " + key + " for DAE HTTPHandler. ", ErrorCodes::UNKNOWN_FUNCTION);

        return dyn_acthion_map[key]();
    }

    static std::unique_ptr<IRestAction> produceUnique(const String& key)
    {
        return std::unique_ptr<IRestAction>(produce(key));
    }

    static std::shared_ptr<IRestAction> produceShared(const String& key)
    {
        return std::shared_ptr<IRestAction>(produce(key));
    }

private:

    RestActionFactory(){}
    RestActionFactory(const RestActionFactory&) = delete;
    RestActionFactory(RestActionFactory&&) = delete;

    static RestActionFactory& get()
    {
        static RestActionFactory instance;
        return instance;
    }

    static std::map<String, std::function<IRestAction*()>> dyn_acthion_map;
};

#define REGISTER_ACTION_VNAME(T) regist_action_##T##_
#define REGISTER_IREATACTION(key, T) static RestActionFactory::ActionRegister<T> REGISTER_ACTION_VNAME(T)(key);
}

