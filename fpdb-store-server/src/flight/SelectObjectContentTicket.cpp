//
// Created by Yifei Yang on 2/23/22.
//

#include "fpdb/store/server/flight/SelectObjectContentTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

SelectObjectContentTicket::SelectObjectContentTicket(long query_id,
                                                     const std::string &fpdb_store_super_pop,
                                                     const std::string &query_plan_string):
  TicketObject(TicketType::select_object_content()),
  query_id_(query_id),
  fpdb_store_super_pop_(fpdb_store_super_pop),
  query_plan_string_(query_plan_string) {}

std::shared_ptr<SelectObjectContentTicket> SelectObjectContentTicket::make(long query_id,
                                                                           const std::string &fpdb_store_super_pop,
                                                                           const std::string &query_plan_string) {
  return std::make_shared<SelectObjectContentTicket>(query_id, fpdb_store_super_pop, query_plan_string);
}

long SelectObjectContentTicket::query_id() const {
  return query_id_;
}

const std::string &SelectObjectContentTicket::fpdb_store_super_pop() const {
  return fpdb_store_super_pop_;
}

const std::string &SelectObjectContentTicket::query_plan_string() const {
  return query_plan_string_;
}

tl::expected<std::string, std::string> SelectObjectContentTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(FPDBStoreSuperPOpJSONName.data(), fpdb_store_super_pop_);
  document.emplace(QueryPlanJSONName.data(), query_plan_string_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<SelectObjectContentTicket>, std::string>
SelectObjectContentTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in SelectObjectContentTicket JSON '{}'", jObj));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(FPDBStoreSuperPOpJSONName.data())) {
    return tl::make_unexpected(fmt::format("FPDBStoreSuperPOp not specified in SelectObjectContentTicket JSON '{}'", jObj));
  }
  auto fpdb_store_super_pop = jObj[FPDBStoreSuperPOpJSONName.data()].get<std::string>();

  if (!jObj.contains(QueryPlanJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query plan not specified in SelectObjectContentTicket JSON '{}'", jObj));
  }
  auto query_plan_string = jObj[QueryPlanJSONName.data()].get<std::string>();

  return SelectObjectContentTicket::make(query_id, fpdb_store_super_pop, query_plan_string);
}

}
