//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H

#include <normal/executor/physical/POpRelationshipType.h>
#include <caf/all.hpp>
#include <string>

namespace normal::executor::physical {

/**
 * An entry in the local physical operator directory
 */
class LocalPOpDirectoryEntry {

private:
  std::string name_;
  ::caf::actor actor_;
  POpRelationshipType relationshipType_;
  bool complete_;

public:
  LocalPOpDirectoryEntry(std::string name,
                         ::caf::actor actor,
                         POpRelationshipType relationshipType,
                         bool complete);


  bool complete() const;
  void complete(bool complete);

  const std::string &name() const;
  void name(const std::string &name);
  const ::caf::actor &getActor() const;
  void destroyActor();
  POpRelationshipType relationshipType() const;
  void relationshipType(POpRelationshipType relationshipType);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H
