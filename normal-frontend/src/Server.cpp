//
// Created by Yifei Yang on 1/14/22.
//

#include <normal/frontend/Server.h>
#include <normal/frontend/ExecConfig.h>
#include <normal/frontend/CAFInit.h>
#include <normal/aws/AWSClient.h>
#include <normal/util/Util.h>
#include <iostream>

using namespace normal::util;

namespace normal::frontend {

void Server::start() {
  // start the daemon AWS client
  const auto &awsConfig = AWSConfig::parseAWSConfig();
  normal::aws::AWSClient::daemonClient_ = make_shared<AWSClient>(awsConfig);
  normal::aws::AWSClient::daemonClient_->init();
  SPDLOG_INFO("Daemon AWS client started");

  // read remote Ips and server port
  const auto &remoteIps = readRemoteIps();
  int CAFServerPort = ExecConfig::parseCAFServerPort();

  // create the actor system
  CAFInit::initCAFGlobalMetaObjects();
  actorSystemConfig_ = std::make_shared<ActorSystemConfig>(CAFServerPort, remoteIps, true);
  actorSystem_ = std::make_shared<::caf::actor_system>(*actorSystemConfig_);

  // open the port
  auto res = actorSystem_->middleman().open(actorSystemConfig_->port_);
  if (!res) {
    throw std::runtime_error("Cannot open CAF server at port: " + to_string(res.error()));
  } else {
    std::cout << "CAF server opened at port: " << actorSystemConfig_->port_ << std::endl;
  }

  std::cout << "Server started" << std::endl;
}

void Server::stop() {
  // stop the daemon AWS client
  normal::aws::AWSClient::daemonClient_->shutdown();
  SPDLOG_INFO("Daemon AWS client stopped");

  if (actorSystem_) {
    auto res = actorSystem_->middleman().close(actorSystemConfig_->port_);
    if (!res) {
      throw std::runtime_error("Cannot close CAF server at port: " + to_string(res.error()));
    }
  }
  std::cout << "CAF server closed at port: " << actorSystemConfig_->port_ << std::endl;

  std::cout << "Server stopped" << std::endl;
}

}
