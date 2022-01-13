//
// Created by matt on 9/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H

#include <string>

namespace normal::executor::message {

/**
 * Base class for messages
 */
class Message {

public:
  explicit Message(std::string type, std::string sender);
  Message() = default;
  Message(const Message&) = default;
  Message& operator=(const Message&) = default;
  virtual ~Message() = default;

  const std::string& type() const;
  const std::string& sender() const;

  // caf inspector needs non-const field type, so the following are needed when inspecting derived message types
  std::string& typeNoConst();
  std::string& senderNoConst();

private:
  std::string type_;
  std::string sender_;

};

} // namespace

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H
