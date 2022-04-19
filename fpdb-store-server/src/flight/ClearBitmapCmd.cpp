//
// Created by Yifei Yang on 4/18/22.
//

#include "fpdb/store/server/flight/ClearBitmapCmd.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

ClearBitmapCmd::ClearBitmapCmd(long query_id, const std::string &op, bool is_compute_side):
  CmdObject(CmdType::clear_bitmap()),
  query_id_(query_id),
  op_(op),
  is_compute_side_(is_compute_side) {}

std::shared_ptr<ClearBitmapCmd> ClearBitmapCmd::make(long query_id, const std::string &op, bool is_compute_side) {
  return std::make_shared<ClearBitmapCmd>(query_id, op, is_compute_side);
}

long ClearBitmapCmd::query_id() const {
  return query_id_;
}

const std::string& ClearBitmapCmd::op() const {
  return op_;
}

bool ClearBitmapCmd::is_compute_side() const {
  return is_compute_side_;
}

tl::expected<std::string, std::string> ClearBitmapCmd::serialize(bool pretty) {
  nlohmann::json value;
  value.emplace(TypeJSONName.data(), type()->name());
  value.emplace(QueryIdJSONName.data(), query_id_);
  value.emplace(OpJSONName.data(), op_);
  value.emplace(IsComputeSideJsonName.data(), is_compute_side_);
  return value.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<ClearBitmapCmd>, std::string> ClearBitmapCmd::from_json(const nlohmann::json& jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<long>();

  if (!jObj.contains(OpJSONName.data())) {
    return tl::make_unexpected(fmt::format("Op not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto op = jObj[OpJSONName.data()].get<std::string>();

  if (!jObj.contains(IsComputeSideJsonName.data())) {
    return tl::make_unexpected(fmt::format("Is compute side not specified in PutBitmapCmd JSON '{}'", jObj));
  }
  auto is_compute_side = jObj[IsComputeSideJsonName.data()].get<bool>();

  return ClearBitmapCmd::make(query_id, op, is_compute_side);
}

}
