//
// Created by matt on 10/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP

#include <future>
#include <memory>

#include "flight/FlightHandler.hpp"
#include "SignalHandler.hpp"
#include "caf/ActorManager.hpp"

namespace fpdb::store::server {

using namespace fpdb::store::server::flight;

class Server {
public:
  Server(std::string name, int flight_port, std::shared_ptr<caf::ActorManager> ActorManager);
  virtual ~Server();

  static std::shared_ptr<Server> make(const std::string& name, int port, std::optional<std::shared_ptr<caf::ActorManager>> optional_actor_manager = std::nullopt);

  [[nodiscard]] int flight_port() const;
  [[nodiscard]] bool running() const;

  [[nodiscard]] tl::expected<void, std::string> init();
  [[nodiscard]] tl::expected<void, std::string> start();
  void stop();
  void join();

private:

  void stop_except_signal_handler();

  bool running_ = false;

  std::string name_ = 0;
  int flight_port_ = 0;

  std::unique_ptr<SignalHandler> signal_handler_;
  std::unique_ptr<FlightHandler> flight_handler_;
  std::shared_ptr<caf::ActorManager> actor_manager_;

  std::future<tl::expected<void, std::basic_string<char>>> flight_future_;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
