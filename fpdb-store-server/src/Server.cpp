//
// Created by matt on 10/2/22.
//

#include "fpdb/store/server/Server.hpp"

#include <future>

#include "Global.hpp"

namespace fpdb::store::server {

using namespace std::chrono_literals;

Server::Server(int flight_port) : flight_port_(flight_port) {
}

Server::~Server() {
  if(running_) {
    stop();
  }
}

tl::expected<void, std::string> Server::init() {

  ::arrow::Status st;

  signal_handler_ = std::make_unique<SignalHandler>([&](int signum) {
    SPDLOG_INFO("Received signal (Signal {}:{})", signum, strsignal(signum));
    stop_except_signal_handler();
  });
  signal_handler_->start();

  // Init the flight handler
  ::arrow::flight::Location server_location;
  st = ::arrow::flight::Location::ForGrpcTcp("0.0.0.0", flight_port_, &server_location);
  if(!st.ok()) {
    return tl::make_unexpected(fmt::format("Could not start FlightHandler, {}", st.message()));
  }
  flight_handler_ = std::make_unique<FlightHandler>(server_location);
  auto ex = flight_handler_->init();
  if(!ex.has_value()) {
    return tl::make_unexpected(fmt::format("Could not start FlightHandler, {}", st.message()));
  }

  flight_port_ = flight_handler_->port();

  return {};
}

tl::expected<void, std::string> Server::start() {

  assert(!running_);

  SPDLOG_INFO("FlexPushdownDB Store Server starting");

  flight_future_ = std::async(std::launch::async, [=]() { return flight_handler_->serve(); });

  // Bit of a hack to check if the flight server failed on "serve"
  if(flight_future_.wait_for(100ms) == std::future_status::ready) {
    return tl::make_unexpected(flight_future_.get().error());
  }

  running_ = true;

  SPDLOG_INFO("FlexPushdownDB Store Server started (FlightHandler listening on {})", flight_port_);

  return {};
}

void Server::stop() {

  assert(running_);

  SPDLOG_INFO("FlexPushdownDB Store Server stopping");

  flight_handler_->shutdown();
  flight_handler_->wait();
  flight_future_.wait();

  signal_handler_->stop();

  running_ = false;

  SPDLOG_INFO("FlexPushdownDB Store Server stopped ");
}

void Server::join() {
  flight_handler_->wait();
  flight_future_.wait();
}

int Server::flight_port() const {
  return flight_port_;
}

bool Server::running() const {
  return running_;
}

void Server::stop_except_signal_handler() {

  assert(running_);

  SPDLOG_INFO("FlexPushdownDB Store Server stopping");

  flight_handler_->shutdown();
  flight_handler_->wait();
  flight_future_.wait();

  running_ = false;

  SPDLOG_INFO("FlexPushdownDB Store Server stopped ");
}

} // namespace fpdb::store::server
