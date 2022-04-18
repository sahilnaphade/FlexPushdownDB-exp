//
// Created by Yifei Yang on 4/18/22.
//

#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical {

void makeDoPutFlightClient(const std::string &host, int port) {
  std::lock_guard<std::mutex> g(DoPutFlightClientLock);

  if (DoPutFlightClient.has_value()) {
    return;
  }

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

  DoPutFlightClient = std::move(client);
}

void clearGlobal() {
  DoPutFlightClient.reset();
}

}
