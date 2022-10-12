//
// Created by Yifei Yang on 7/3/22.
//

#include <fpdb/executor/message/TupleSetReadyRemoteMessage.h>

namespace fpdb::executor::message {

TupleSetReadyRemoteMessage::TupleSetReadyRemoteMessage(const std::string &host,
                                                       int port,
                                                       const std::string &sender) :
  Message(TUPLESET_READY_REMOTE, sender),
  host_(host),
  port_(port) {}

std::string TupleSetReadyRemoteMessage::getTypeString() const {
  return "TupleSetReadyRemoteMessage";
}

const std::string &TupleSetReadyRemoteMessage::getHost() const {
  return host_;
}

int TupleSetReadyRemoteMessage::getPort() const {
  return port_;
}

}
