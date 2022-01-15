//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
#define NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H

#include <normal/executor/physical/POpActor.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <caf/all.hpp>
#include <caf/io/all.hpp>

using namespace normal::executor::physical;

namespace normal::frontend {

struct ClientActorSystemConfig: ::caf::actor_system_config {
  ClientActorSystemConfig(int port,
                          const std::vector<std::string> &nodeIps):
    port_(port),
    nodeIps_(nodeIps) {
    load<::caf::io::middleman>();
//    add_actor_type<POpActor, ::caf::actor_config, std::shared_ptr<PhysicalOp>>("POpActor");
  }

  int port_;
  std::vector<std::string> nodeIps_;
};

};


#endif //NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
