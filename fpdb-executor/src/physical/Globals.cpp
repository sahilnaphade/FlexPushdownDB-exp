//
// Created by Yifei Yang on 4/18/22.
//

#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical {

arrow::flight::FlightClient* makeDoPutFlightClient(const std::string &host, int port) {
  std::lock_guard<std::mutex> g(DoPutFlightClientLock);

  // check if already made before
  auto clientIt = DoPutFlightClients.find(host);
  if (clientIt != DoPutFlightClients.end()) {
    return clientIt->second.get();
  }

  // if not made yet, make one and save
  arrow::flight::Location clientLocation;
  auto status = arrow::flight::Location::ForGrpcTcp(host, port, &clientLocation);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  arrow::flight::FlightClientOptions clientOptions = arrow::flight::FlightClientOptions::Defaults();
  std::unique_ptr<arrow::flight::FlightClient> client;
  status = arrow::flight::FlightClient::Connect(clientLocation, clientOptions, &client);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  auto clientRawPtr = client.get();
  DoPutFlightClients[host] = std::move(client);
  return clientRawPtr;
}

void clearGlobal() {
  DoPutFlightClients.clear();
}

}
