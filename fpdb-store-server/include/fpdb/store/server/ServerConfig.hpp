//
// Created by matt on 16/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP

#include <filesystem>
#include <optional>

namespace fpdb::store::server {

class ServerConfig {
public:
  std::string name;
  int node_port;

  bool start_coordinator;
  std::optional<std::string> coordinator_host;
  std::optional<int> coordinator_port;

  int flight_port;

  std::filesystem::path data_path;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP