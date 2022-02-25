//
// Created by Yifei Yang on 2/23/22.
//

#include "fpdb/store/server/flight/SelectObjectContentTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

SelectObjectContentTicket::SelectObjectContentTicket(std::string query_plan_string)
        : TicketObject(TicketType::select_object_content()),
          query_plan_string_(std::move(query_plan_string)) {
}

std::shared_ptr<SelectObjectContentTicket> SelectObjectContentTicket::make(std::string query_plan_string) {
  return std::make_shared<SelectObjectContentTicket>(std::move(query_plan_string));
}

const std::string & SelectObjectContentTicket::query_plan_string() const {
  return query_plan_string_;
}

tl::expected<std::string, std::string> SelectObjectContentTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryPlanJSONName.data(), query_plan_string_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<SelectObjectContentTicket>, std::string>
SelectObjectContentTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryPlanJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query plan not specified in GetObjectTicket JSON '{}'", jObj));
  }
  auto query_plan_string = jObj[QueryPlanJSONName.data()].get<std::string>();

  return SelectObjectContentTicket::make(query_plan_string);
}

}
