//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONTEXT_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONTEXT_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/POpActor.h>
#include <normal/executor/physical/LocalPOpDirectory.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/physical/Forward.h>
#include <memory>
#include <string>

namespace normal::executor::physical {

/**
 * The API physical operators use to interact with their environment, e.g. sending messages
 */
class POpContext {
private:
  POpActor* operatorActor_;
  LocalPOpDirectory operatorMap_;
  caf::actor rootActor_;
  caf::actor segmentCacheActor_;
  bool complete_ = false;

public:
  POpContext(caf::actor rootActor, caf::actor segmentCacheActor);

  POpActor* operatorActor();
  void operatorActor(POpActor *operatorActor);

  LocalPOpDirectory &operatorMap();

  void tell(std::shared_ptr<message::Message> &msg);

  void notifyComplete();

  tl::expected<void, std::string> send(const std::shared_ptr<message::Message> &msg, const std::string &recipientId);

  void destroyActorHandles();
  [[nodiscard]] bool isComplete() const;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPCONTEXT_H
