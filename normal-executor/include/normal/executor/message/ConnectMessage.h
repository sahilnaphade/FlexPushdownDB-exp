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

private:
  std::vector<POpConnection> connections_;

public:
  explicit ConnectMessage(std::vector<POpConnection> operatorConnections, std::string from);
  const std::vector<POpConnection> &connections() const;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
