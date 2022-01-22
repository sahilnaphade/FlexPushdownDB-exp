//
// Created by matt on 9/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H

#include <normal/executor/message/MessageType.h>
#include <string>

namespace normal::executor::message {

/**
 * Base class for messages
 */
class Message {

public:
  explicit Message(MessageType type, std::string sender);
  Message() = default;
  Message(const Message&) = default;
  Message& operator=(const Message&) = default;
  virtual ~Message() = default;

  MessageType type() const;
  const std::string& sender() const;
  virtual std::string getTypeString() const = 0;

protected:
  MessageType type_;
  std::string sender_;

};

} // namespace

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H
