//
// Created by Yifei Yang on 4/17/22.
//

#include "fpdb/store/server/flight/PutBitmapCmd.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

PutBitmapCmd::PutBitmapCmd(long query_id, const std::string &op, bool valid):
  CmdObject(CmdType::put_bitmap()),
  query_id_(query_id),
  op_(op),
  valid_(valid) {}

std::shared_ptr<PutBitmapCmd> PutBitmapCmd::make(long query_id, const std::string &op, bool valid) {
  return std::make_shared<PutBitmapCmd>(query_id, op, valid);
}

long PutBitmapCmd::query_id() const {
  return query_id_;
}

const std::string& PutBitmapCmd::op() const {
  return op_;
}

bool PutBitmapCmd::valid() const {
  return valid_;
}

tl::expected<std::string, std::string> PutBitmapCmd::serialize(bool pretty) {
  nlohmann::json value;
  value.emplace(TypeJSONName.data(), type()->name());
  value.emplace(QueryIdJSONName.data(), query_id_);
  value.emplace(OpJSONName.data(), op_);
  value.emplace(ValidJSONName.data(), valid_);
  return value.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<PutBitmapCmd>, std::string> PutBitmapCmd::from_json(const nlohmann::json& jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<long>();

  if (!jObj.contains(OpJSONName.data())) {
    return tl::make_unexpected(fmt::format("Op not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto op = jObj[OpJSONName.data()].get<std::string>();

  if (!jObj.contains(ValidJSONName.data())) {
    return tl::make_unexpected(fmt::format("Valid not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto valid = jObj[ValidJSONName.data()].get<bool>();

  return PutBitmapCmd::make(query_id, op, valid);
}

}
