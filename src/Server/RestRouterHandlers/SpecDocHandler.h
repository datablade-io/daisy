#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class SpecDocHandler final : public RestRouterHandler
{
public:
   explicit SpecDocHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "SpecDoc") { }
   ~SpecDocHandler() override { }

private:
   std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
   void buildResponse(const Block & block, String & resp) const;
};

}
