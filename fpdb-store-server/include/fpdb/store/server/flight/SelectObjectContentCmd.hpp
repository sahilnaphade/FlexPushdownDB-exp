//
// Created by Yifei Yang on 2/23/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTCMD_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTCMD_HPP

#include "fpdb/store/server/flight/CmdObject.hpp"

namespace fpdb::store::server::flight {

class SelectObjectContentCmd : public CmdObject {
public:
  explicit SelectObjectContentCmd(std::string query_plan_string);

  static std::shared_ptr<SelectObjectContentCmd> make(std::string query_plan_string);

  [[nodiscard]] const std::string& query_plan_string() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<SelectObjectContentCmd>, std::string> from_json(const nlohmann::json& jObj);

private:
  std::string query_plan_string_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTCMD_HPP
