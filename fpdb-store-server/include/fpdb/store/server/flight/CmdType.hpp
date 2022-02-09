//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDTYPE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDTYPE_HPP

#include <memory>
#include <string>

namespace fpdb::store::server::flight {

enum class CmdTypeId { SelectObjectContent };

class CmdType {
public:
  CmdType(CmdTypeId id, std::string name);

  [[nodiscard]] CmdTypeId id() const;

  [[nodiscard]] const std::string& name() const;

  static std::shared_ptr<CmdType> select_object_content();

private:
  CmdTypeId id_;
  std::string name_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDTYPE_HPP
