//
// Created by matt on 5/3/20.
//

#include <normal/executor/message/CompleteMessage.h>
#include <utility>

namespace normal::executor::message {

CompleteMessage::CompleteMessage(std::string sender) : Message("CompleteMessage", std::move(sender)) {}

}