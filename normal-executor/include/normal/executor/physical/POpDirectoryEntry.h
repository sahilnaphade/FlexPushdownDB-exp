//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H

#include <normal/executor/physical/POpContext.h>
#include <caf/all.hpp>
#include <string>

namespace normal::executor::physical {

/**
 * Entry in the physical operator directory
 */
class POpDirectoryEntry {

private:
  std::shared_ptr<PhysicalOp> def_;
  caf::actor actorHandle_;
  bool complete_;

public:
  POpDirectoryEntry(std::shared_ptr<PhysicalOp> def, caf::actor actorHandle, bool complete);
  [[nodiscard]] const std::shared_ptr<PhysicalOp> &getDef() const;
  [[nodiscard]] const caf::actor &getActorHandle() const;
  [[nodiscard]] bool isComplete() const;
  void setDef(const std::shared_ptr<PhysicalOp> &def);
  void setActorHandle(const caf::actor &actorHandle);
  void setComplete(bool complete);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H
