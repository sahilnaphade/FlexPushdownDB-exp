//
// Created by matt on 10/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP

#include <future>
#include <memory>

#include "ServerConfig.hpp"
#include "SignalHandler.hpp"
#include "caf/ActorManager.hpp"
#include "caf/ServerMeta.hpp"
#include "flight/FlightHandler.hpp"

namespace fpdb::store::server {

using namespace fpdb::store::server::flight;
using namespace fpdb::store::server::cluster;

class Server {
public:
  Server(const ServerConfig& cfg, std::optional<ClusterActor> coordinator_actor_handle,
         std::shared_ptr<caf::ActorManager> actor_manager);
  virtual ~Server();

  static std::shared_ptr<Server>
  make(const ServerConfig& cfg, const std::optional<ClusterActor>& coordinator_actor_handle = std::nullopt,
       std::optional<std::shared_ptr<caf::ActorManager>> optional_actor_manager = std::nullopt);

  [[nodiscard]] const std::optional<int>& coordinator_port() const;
  [[nodiscard]] int flight_port() const;
  [[nodiscard]] bool running() const;
  [[nodiscard]] const std::optional<ClusterActor>& cluster_actor_handle() const;

  [[nodiscard]] tl::expected<void, std::string> init();
  [[nodiscard]] tl::expected<void, std::string> start();
  void stop();
  void join();

private:
  void stop_except_signal_handler();

  bool running_ = false;

  // config
  std::string name_ = "<no-name>";
  int node_port_ = 0;
  bool start_coordinator_ = false;
  std::optional<std::string> coordinator_host_ = std::nullopt;
  std::optional<int> coordinator_port_ = 0;
  int flight_port_ = 0;
  std::string store_root_path_;

  std::unique_ptr<SignalHandler> signal_handler_;
  std::unique_ptr<FlightHandler> flight_handler_;
  std::shared_ptr<caf::ActorManager> actor_manager_;
  NodeActor node_actor_handle_;
  std::optional<ClusterActor> cluster_actor_handle_;

  std::future<tl::expected<void, std::basic_string<char>>> flight_future_;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVER_HPP
