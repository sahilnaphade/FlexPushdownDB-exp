//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/CmdObject.hpp"
#include "fpdb/store/server/flight/SelectObjectContentCmd.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include <fmt/format.h>

namespace fpdb::store::server::flight {

CmdObject::CmdObject(std::shared_ptr<CmdType> type) : type_(std::move(type)) {}

tl::expected<std::shared_ptr<CmdObject>, std::string> CmdObject::deserialize(const std::string& cmd_string) {
  auto document = nlohmann::json::parse(cmd_string);
  if (!document.is_object()) {
    return tl::make_unexpected(fmt::format("Cannot parse CmdObject JSON '{}'", cmd_string));
  }

  if (!document.contains(TypeJSONName.data())) {
    return tl::make_unexpected(fmt::format("Type not specified in CmdObject JSON '{}'", cmd_string));
  }
  auto type = document[TypeJSONName.data()].get<std::string>();

  if (type == SelectObjectCmdTypeName.data()) {
    return SelectObjectContentCmd::from_json(document);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported Ticket object type: '{}'", type));
  }
}

const std::shared_ptr<CmdType>& CmdObject::type() const {
  return type_;
}

} // namespace fpdb::store::server::flight