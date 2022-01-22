//
// Created by Yifei Yang on 1/21/22.
//

#include <normal/executor/message/ErrorMessage.h>

namespace normal::executor::message {

ErrorMessage::ErrorMessage(const std::string &content, const std::string &sender) :
  Message(ERROR, sender),
  content_(content) {}

std::string ErrorMessage::getTypeString() const {
  return "ErrorMessage";
}

const std::string &ErrorMessage::getContent() const {
  return content_;
}

}
