//
// Created by matt on 23/9/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONNECTION_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONNECTION_H

#include <normal/executor/physical/POpRelationshipType.h>
#include <caf/all.hpp>
#include <string>

namespace normal::executor::physical {

class POpConnection {
public:
  POpConnection(std::string Name, caf::actor ActorHandle, POpRelationshipType ConnectionType);

  const std::string &getName() const;
  const caf::actor &getActorHandle() const;
  POpRelationshipType getConnectionType() const;

private:
  std::string name_;
  caf::actor actorHandle_;
  POpRelationshipType connectionType_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONNECTION_H
