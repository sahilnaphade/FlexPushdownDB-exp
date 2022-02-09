//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETTYPE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETTYPE_HPP

#include <memory>
#include <string>

namespace fpdb::store::server::flight {

enum class TicketTypeId { GetObject, SelectObjectContent };

class TicketType {
public:
  TicketType(TicketTypeId id, std::string name);

  [[nodiscard]] TicketTypeId id() const;

  [[nodiscard]] const std::string& name() const;

  static std::shared_ptr<TicketType> get_object();

  static std::shared_ptr<TicketType> select_object_content();

private:
  TicketTypeId id_;
  std::string name_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETTYPE_HPP
