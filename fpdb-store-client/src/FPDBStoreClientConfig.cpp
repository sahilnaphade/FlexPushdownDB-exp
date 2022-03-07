//
// Created by Yifei Yang on 3/7/22.
//

#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <fpdb/util/Util.h>
#include <unordered_map>

using namespace fpdb::util;

namespace fpdb::store::client {

FPDBStoreClientConfig::FPDBStoreClientConfig(const std::string &host,
                                             int fileServicePort,
                                             int flightPort):
  host_(host),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

std::shared_ptr<FPDBStoreClientConfig> FPDBStoreClientConfig::parseFPDBStoreClientConfig() {
  std::unordered_map<string, string> configMap = readConfig("fpdb-store.conf");
  auto host = configMap["HOST"];
  auto fileServicePort = std::stoi(configMap["FILE_SERVICE_PORT"]);
  auto flightPort = std::stoi(configMap["FLIGHT_PORT"]);
  return std::make_shared<FPDBStoreClientConfig>(host, fileServicePort, flightPort);
}

const std::string &FPDBStoreClientConfig::getHost() const {
  return host_;
}

int FPDBStoreClientConfig::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreClientConfig::getFlightPort() const {
  return flightPort_;
}

}
