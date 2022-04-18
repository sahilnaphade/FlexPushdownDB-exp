//
// Created by Yifei Yang on 4/17/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP

#include "fpdb/store/server/flight/CmdObject.hpp"

namespace fpdb::store::server::flight {

class PutBitmapCmd : public CmdObject {

public:
  PutBitmapCmd(long query_id, const std::string &op, bool valid);

  static std::shared_ptr<PutBitmapCmd> make(long query_id, const std::string &op, bool valid);

  long query_id() const;
  const std::string& op() const;
  bool valid() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;
  static tl::expected<std::shared_ptr<PutBitmapCmd>, std::string> from_json(const nlohmann::json& jObj);

private:
  long query_id_;
  std::string op_;
  bool valid_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP
