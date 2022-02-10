//
// Created by matt on 10/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP

#include <future>
#include <memory>

#include "flight/FlightHandler.hpp"
#include "SignalHandler.hpp"

namespace fpdb::store::server {

using namespace fpdb::store::server::flight;

class Server {
public:
  explicit Server(int flight_port);
  virtual ~Server();

  [[nodiscard]] int flight_port() const;
  [[nodiscard]] bool running() const;

  [[nodiscard]] tl::expected<void, std::string> init();
  [[nodiscard]] tl::expected<void, std::string> start();
  void stop();
  void join();

private:

  void stop_except_signal_handler();

  bool running_ = false;

  int flight_port_ = 0;

  std::unique_ptr<SignalHandler> signal_handler_;
  std::unique_ptr<FlightHandler> flight_handler_;

  std::future<tl::expected<void, std::basic_string<char>>> flight_future_;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
