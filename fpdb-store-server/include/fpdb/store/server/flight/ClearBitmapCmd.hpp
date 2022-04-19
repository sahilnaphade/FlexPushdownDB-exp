//
// Created by Yifei Yang on 4/18/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CLEARBITMAPCMD_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CLEARBITMAPCMD_HPP

#include "fpdb/store/server/flight/CmdObject.hpp"

namespace fpdb::store::server::flight {

class ClearBitmapCmd : public CmdObject {

public:
  ClearBitmapCmd(long query_id, const std::string &op, bool is_compute_side);

  static std::shared_ptr<ClearBitmapCmd> make(long query_id, const std::string &op, bool is_compute_side);

  long query_id() const;
  const std::string& op() const;
  bool is_compute_side() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;
  static tl::expected<std::shared_ptr<ClearBitmapCmd>, std::string> from_json(const nlohmann::json& jObj);

private:
  long query_id_;
  std::string op_;
  bool is_compute_side_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CLEARBITMAPCMD_HPP
