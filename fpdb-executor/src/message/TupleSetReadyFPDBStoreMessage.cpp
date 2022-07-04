//
// Created by Yifei Yang on 7/3/22.
//

#include <fpdb/executor/message/TupleSetReadyFPDBStoreMessage.h>

namespace fpdb::executor::message {

TupleSetReadyFPDBStoreMessage::TupleSetReadyFPDBStoreMessage(const std::string &host,
                                                             int port,
                                                             const std::string &sender) :
  Message(TUPLESET_READY_FPDB_STORE, sender),
  host_(host),
  port_(port) {}

std::string TupleSetReadyFPDBStoreMessage::getTypeString() const {
  return "TupleSetReadyFPDBStoreMessage";
}

const std::string &TupleSetReadyFPDBStoreMessage::getHost() const {
  return host_;
}

int TupleSetReadyFPDBStoreMessage::getPort() const {
  return port_;
}

}
