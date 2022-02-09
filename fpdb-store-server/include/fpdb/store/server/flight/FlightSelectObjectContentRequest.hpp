//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTSELECTOBJECTCONTENTREQUEST_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTSELECTOBJECTCONTENTREQUEST_HPP

#include <memory>
#include <string>

#include <nlohmann/json.hpp>
#include <tl/expected.hpp>

#include "fpdb/store/server/flight/FlightInputSerialization.hpp"

namespace fpdb::store::server::flight {

class FlightSelectObjectContentRequest {
public:
  FlightSelectObjectContentRequest(std::string expression,
                                   std::shared_ptr<FlightInputSerialization> input_serialization);

  static std::shared_ptr<FlightSelectObjectContentRequest>
  make(const std::string& expression, std::shared_ptr<FlightInputSerialization> input_serialization);

  [[nodiscard]] const std::string& expression() const;

  [[nodiscard]] const std::shared_ptr<FlightInputSerialization>& input_serialization() const;

  nlohmann::json to_json();

  std::string serialize(bool pretty = false);

  static tl::expected<std::shared_ptr<FlightSelectObjectContentRequest>, std::string>
  from_json(const nlohmann::json& json);

  static tl::expected<std::shared_ptr<FlightSelectObjectContentRequest>, std::string>
  deserialize(const std::string& command_string);

private:
  std::string expression_;
  std::shared_ptr<FlightInputSerialization> input_serialization_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_FLIGHTSELECTOBJECTCONTENTREQUEST_HPP
