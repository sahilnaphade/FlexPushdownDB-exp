//
// Created by Yifei Yang on 7/4/22.
//

#include "fpdb/store/server/flight/GetTableTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

GetTableTicket::GetTableTicket(long query_id, const std::string &producer, const std::string &consumer):
  TicketObject(TicketType::get_table()),
  query_id_(query_id),
  producer_(producer),
  consumer_(consumer) {}

std::shared_ptr<GetTableTicket>
GetTableTicket::make(long query_id, const std::string &producer, const std::string &consumer) {
  return std::make_shared<GetTableTicket>(query_id, producer, consumer);
}

long GetTableTicket::query_id() const {
  return query_id_;
}

const std::string& GetTableTicket::producer() const {
  return producer_;
}

const std::string& GetTableTicket::consumer() const {
  return consumer_;
}

tl::expected<std::string, std::string> GetTableTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(ProducerJSONName.data(), producer_);
  document.emplace(ConsumerJSONName.data(), consumer_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetTableTicket>, std::string> GetTableTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in GetTableTicket JSON '{}'", jObj));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(ProducerJSONName.data())) {
    return tl::make_unexpected(fmt::format("Producer not specified in GetTableTicket JSON '{}'", jObj));
  }
  auto producer = jObj[ProducerJSONName.data()].get<std::string>();

  if (!jObj.contains(ConsumerJSONName.data())) {
    return tl::make_unexpected(fmt::format("Consumer not specified in GetTableTicket JSON '{}'", jObj));
  }
  auto consumer = jObj[ConsumerJSONName.data()].get<std::string>();

  return GetTableTicket::make(query_id, producer, consumer);
}

}
