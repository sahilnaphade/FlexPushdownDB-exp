//
// Created by matt on 4/2/22.
//

#include "Global.hpp"

#include "fpdb/store/server/Server.hpp"

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;


int main(int /*argc*/, char** /*argv*/) {

  auto server = Server::make(ServerConfig{"node1", 10000, true, std::nullopt, 10001,  10002, "./data"}, std::nullopt,
                             std::nullopt);

  // Start the server
  auto init_result = server->init();
  if(!init_result.has_value()){
    SPDLOG_ERROR("Could not start Store Server, {}", init_result.error());
  }
  auto start_result = server->start();
  if(!start_result.has_value()){
    SPDLOG_ERROR("Could not start Store Server, {}", start_result.error());
  }

  server->join();

  return 0;
}