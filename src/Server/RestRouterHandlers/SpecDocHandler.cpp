#include "SpecDocHandler.h"

#include <Common/Config/YAMLParser.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Path.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
}

namespace
{
String getSpecPath(const String & config_path)
{
    String spec_path = "";
    Poco::Path path(config_path);

    if (!path.parent().toString().empty() && Poco::File(path.parent().toString() + "/clickhouse-spec").exists())
    {
        spec_path = path.parent().toString() + "/clickhouse-spec/";
    }
    else
    {
        String home_path = getenv("HOME");
        if (Poco::File(path.current() + "/spec/").exists())
        {
            spec_path = path.current() + "/spec/";
        }
        else if (!home_path.empty() && Poco::File(home_path + "/clickhouse-spec/").exists())
        {
            spec_path = home_path + "/clickhouse-spec/";
        }
        else if (Poco::File("/etc/clickhouse-spec/").exists())
        {
            spec_path = "/etc/clickhouse-spec/";
        }
    }

    return spec_path;
}

}

std::pair<String, Int32> SpecDocHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const String  & spec_path = getSpecPath(query_context->getConfigPath());
    Poco::Path path(spec_path + "rest-api");

    if (!Poco::File(path).exists())
    {
        return {
            jsonErrorResponse(
                "The clickhouse-spec file could not be found, please keep it consistent with the clickhouse-server path",
                ErrorCodes::RESOURCE_NOT_FOUND),
            HTTPResponse::HTTP_NOT_FOUND};
    }

    Poco::JSON::Object doc_json;
    Poco::DirectoryIterator end_iter;
    for (Poco::DirectoryIterator iter(path); iter != end_iter; ++iter)
    {
        if (iter->isFile())
        {
            const auto & doc = YAMLParser::parseToJson(iter->path());
            doc_json.set(iter.path().getBaseName(), doc);
        }
    }

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    doc_json.stringify(resp_str_stream, 0);

    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

}
