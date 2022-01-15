//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
#define NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H

#include <caf/all.hpp>
#include <caf/io/all.hpp>

namespace normal::frontend {

struct ServerActorSystemConfig: ::caf::actor_system_config {
  ServerActorSystemConfig(int port):
    port_(port) {
    load<::caf::io::middleman>();
  }

  int port_;
};

};


#endif //NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_ACTORSYSTEMCONFIG_H
