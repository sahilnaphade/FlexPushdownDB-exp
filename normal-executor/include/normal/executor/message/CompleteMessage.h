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

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H
