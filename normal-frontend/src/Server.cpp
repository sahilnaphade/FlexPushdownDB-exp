//
// Created by Yifei Yang on 1/14/22.
//

#include <normal/frontend/Server.h>
#include <normal/util/Util.h>
#include <iostream>

using namespace normal::util;

namespace normal::frontend {

void Server::start() {
  // read server port
  int CAFServerPort = parseCAFServerPort();

  // create the actor system
  ::caf::core::init_global_meta_objects();
  ::caf::io::middleman::init_global_meta_objects();
  actorSystemConfig_ = std::make_shared<ServerActorSystemConfig>(CAFServerPort);
  actorSystem_ = std::make_shared<::caf::actor_system>(*actorSystemConfig_);

  // open the port
  auto res = actorSystem_->middleman().open(actorSystemConfig_->port_);
  if (!res) {
    throw std::runtime_error("Cannot open CAF server at port: " + to_string(res.error()));
  } else {
    std::cout << "CAF server opened at port: " << actorSystemConfig_->port_ << std::endl;
  }
}

void Server::stop() {
  if (actorSystem_) {
    auto res = actorSystem_->middleman().close(actorSystemConfig_->port_);
    if (!res) {
      throw std::runtime_error("Cannot close CAF server at port: " + to_string(res.error()));
    }
  }
  std::cout << "CAF server closed at port: " << actorSystemConfig_->port_ << std::endl;
}

int Server::parseCAFServerPort() {
  unordered_map<string, string> configMap = readConfig("exec.conf");
  return stoi(configMap["CAF_SERVER_PORT"]);
}

}
