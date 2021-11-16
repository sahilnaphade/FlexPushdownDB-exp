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

private:
  std::string type_;
  std::string sender_;

public:
  explicit Message(std::string type, std::string sender);
  virtual ~Message() = default;
  const std::string& type() const;
  const std::string& sender() const;

};

} // namespace

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_MESSAGE_H
