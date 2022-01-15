//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_SERVER_H
#define NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_SERVER_H

#include <normal/frontend/ServerActorSystemConfig.h>

namespace normal::frontend {

class Server {

public:
  explicit Server() = default;

  void start();
  void stop();

private:
  static int parseCAFServerPort();

  std::shared_ptr<ServerActorSystemConfig> actorSystemConfig_;
  std::shared_ptr<::caf::actor_system> actorSystem_;

};

}


#endif //NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_SERVER_H
