//
// Created by Yifei Yang on 2/23/22.
//

#include "fpdb/store/server/flight/SelectObjectContentCmd.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

SelectObjectContentCmd::SelectObjectContentCmd(std::string query_plan_string)
        : CmdObject(CmdType::select_object_content()),
          query_plan_string_(std::move(query_plan_string)) {
}

std::shared_ptr<SelectObjectContentCmd> SelectObjectContentCmd::make(std::string query_plan_string) {
  return std::make_shared<SelectObjectContentCmd>(std::move(query_plan_string));
}

const std::string& SelectObjectContentCmd::query_plan_string() const {
  return query_plan_string_;
}

tl::expected<std::string, std::string> SelectObjectContentCmd::serialize(bool pretty) {
  nlohmann::json value;
  value.emplace(TypeJSONName.data(), type()->name());
  value.emplace(QueryPlanJSONName.data(), query_plan_string_);
  return value.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<SelectObjectContentCmd>, std::string>
SelectObjectContentCmd::from_json(const nlohmann::json& jObj) {
  if (!jObj.contains(QueryPlanJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query plan not specified in SelectObjectContentCmd JSON '{}'", jObj));
  }
  auto query_plan_string = jObj[QueryPlanJSONName.data()].get<std::string>();

  return SelectObjectContentCmd::make(query_plan_string);
}

}
