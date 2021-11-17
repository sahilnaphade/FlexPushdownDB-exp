//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPACTOR_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPACTOR_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/message/StartMessage.h>
#include <normal/executor/physical/Forward.h>
#include <caf/all.hpp>
#include <memory>
#include <queue>

namespace normal::executor::physical {

using GetProcessingTimeAtom = caf::atom_constant<caf::atom("get-pt")>;

/**
 * Physical operator actor implements caf::actor and combines the physical operators' behaviour and state
 */
class POpActor : public caf::event_based_actor {

public:
  POpActor(caf::actor_config &cfg, std::shared_ptr<PhysicalOp> opBehaviour);

  std::shared_ptr<PhysicalOp> operator_() const;
  caf::behavior make_behavior() override;
  void on_exit() override;
  const char* name() const override {
    return name_.c_str();
  }

  long getProcessingTime() const;
  void incrementProcessingTime(long time);
  bool running_ = false;
  std::string name_;
  std::queue<std::pair<caf::message, caf::strong_actor_ptr>> buffer_;
  std::optional<caf::strong_actor_ptr> overriddenMessageSender_;

private:
  std::shared_ptr<PhysicalOp> opBehaviour_;
  long processingTime_ = 0;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPACTOR_H
