//
// Created by Yifei Yang on 7/4/22.
//

#include "fpdb/store/server/flight/GetTableTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

GetTableTicket::GetTableTicket(long query_id, const std::string &op):
  TicketObject(TicketType::get_table()),
  query_id_(query_id),
  op_(op) {}

std::shared_ptr<GetTableTicket> GetTableTicket::make(long query_id, const std::string &op) {
  return std::make_shared<GetTableTicket>(query_id, op);
}

long GetTableTicket::query_id() const {
  return query_id_;
}

const std::string& GetTableTicket::op() const {
  return op_;
}

tl::expected<std::string, std::string> GetTableTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(OpJSONName.data(), op_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetTableTicket>, std::string> GetTableTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in GetTableTicket JSON '{}'", jObj));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(OpJSONName.data())) {
    return tl::make_unexpected(fmt::format("Op not specified in GetTableTicket JSON '{}'", jObj));
  }
  auto op = jObj[OpJSONName.data()].get<std::string>();

  return GetTableTicket::make(query_id, op);
}

}
