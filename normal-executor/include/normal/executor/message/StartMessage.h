//
// Created by matt on 5/1/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_STARTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_STARTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <caf/all.hpp>
#include <vector>

namespace normal::executor::message {

/**
 * Message sent to operators to tell them to start doing their "thing"
 */
class StartMessage : public Message {

public:
  explicit StartMessage(std::string from);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_STARTMESSAGE_H
