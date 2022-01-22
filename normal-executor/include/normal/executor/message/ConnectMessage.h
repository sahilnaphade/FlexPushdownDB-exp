//
// Created by matt on 30/9/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CONNECTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/executor/physical/POpConnection.h>
#include <vector>

using namespace normal::executor::physical;

namespace normal::executor::message {

/**
 * Message sent to operators to tell them who they are connected to
 */
class ConnectMessage : public Message {

public:
  explicit ConnectMessage(std::vector<POpConnection> operatorConnections, std::string from);
  ConnectMessage() = default;
  ConnectMessage(const ConnectMessage&) = default;
  ConnectMessage& operator=(const ConnectMessage&) = default;

  std::string getTypeString() const override;

  const std::vector<POpConnection> &connections() const;

private:
  std::vector<POpConnection> connections_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ConnectMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("operatorConnections", msg.connections_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
