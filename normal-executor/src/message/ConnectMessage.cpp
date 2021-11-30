//
// Created by matt on 30/9/20.
//

#include <normal/executor/message/ConnectMessage.h>

namespace normal::executor::message {

ConnectMessage::ConnectMessage(std::vector<POpConnection> connections,
							   std::string from) :
	Message("ConnectMessage", std::move(from)),
	connections_(std::move(connections)) {

}

const std::vector<POpConnection> &ConnectMessage::connections() const {
  return connections_;
}

}