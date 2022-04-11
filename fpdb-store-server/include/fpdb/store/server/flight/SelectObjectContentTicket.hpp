//
// Created by Yifei Yang on 2/23/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP

#include "fpdb/store/server/flight/TicketObject.hpp"
#include <nlohmann/json.hpp>

namespace fpdb::store::server::flight {

/**
 * Ticket for executing and returning the results of a query on an object in the store.
 */
class SelectObjectContentTicket : public TicketObject {
public:
  SelectObjectContentTicket(long query_id, std::string query_plan_string);

  static std::shared_ptr<SelectObjectContentTicket> make(long queryId, std::string query_plan_string);

  long query_id() const;
  const std::string& query_plan_string() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<SelectObjectContentTicket>, std::string> from_json(const nlohmann::json &jObj);

private:
  long query_id_;
  std::string query_plan_string_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP
