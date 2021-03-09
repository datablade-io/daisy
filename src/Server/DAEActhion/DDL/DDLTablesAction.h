#pragma once
#include <Poco/JSON/Parser.h>
#include "Server/DAEActhion/IDAEAction.h"

namespace DB
{

class DDLTablesAction : public IDAEAction
{
private:
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;
    static std::map<String, std::map<String, String>> update_schema;
    String getCreateQuery(Poco::JSON::Object::Ptr payload, String db);
    String getUpdateQuery(Poco::JSON::Object::Ptr payload, String db, String table);
    String getColumnsDefination(Poco::JSON::Array::Ptr columns, String time_column);
    String getColumnDefination(Poco::JSON::Object::Ptr column);
    void validateCreateSchema(Poco::JSON::Object::Ptr payload);


    String getParamsJoin(HTTPServerRequest & request, const Poco::Path & path);

    String postParamsJoin(HTTPServerRequest & request, const Poco::Path & path);

    String patchParamsJoin(HTTPServerRequest & request, const Poco::Path & path);

    String deleteParamsJoin(const Poco::Path & path);


public:
    DDLTablesAction(){
        std::cout << "create DDLTablesAction ~ " << std::endl;
    }
    ~DDLTablesAction() override{}

    void execute(
            IServer & server,
            Poco::Logger * log,         
            Context & context,
            HTTPServerRequest & request,
            HTMLForm & params,
            HTTPServerResponse & response,
            Output & used_output,
            std::optional<CurrentThread::QueryScope> & query_scope,
            const Poco::Path & path) override;
};

}
