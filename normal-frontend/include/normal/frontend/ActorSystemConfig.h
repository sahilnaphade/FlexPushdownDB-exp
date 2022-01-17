//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
#define NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H

#include <normal/executor/physical/POpActor.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/serialization/POpSerializer.h>
#include <caf/io/all.hpp>

using namespace normal::executor::physical;

namespace normal::frontend {

struct ActorSystemConfig: ::caf::actor_system_config {
  ActorSystemConfig(int port,
                    const std::vector<std::string> &nodeIps,
                    bool isServer):
    port_(port),
    nodeIps_(nodeIps),
    isServer_(isServer) {
    load<::caf::io::middleman>();
    add_actor_type<POpActor, std::shared_ptr<PhysicalOp>&>("POpActor");
  }

  int port_;
  std::vector<std::string> nodeIps_;
  bool isServer_;
};

};


#endif //NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
