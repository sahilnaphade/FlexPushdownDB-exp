//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H

#include <normal/executor/message/Message.h>

namespace normal::executor::message {
/**
 * Message fired when an operator completes its work
 */
class CompleteMessage : public Message {

public:
  explicit CompleteMessage(std::string sender);
  CompleteMessage() = default;
  CompleteMessage(const CompleteMessage&) = default;
  CompleteMessage& operator=(const CompleteMessage&) = default;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CompleteMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H
