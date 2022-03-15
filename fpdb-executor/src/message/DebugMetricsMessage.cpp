//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/message/DebugMetricsMessage.h>

namespace fpdb::executor::message {

DebugMetricsMessage::DebugMetricsMessage(int64_t bytesFromStore,
                                       std::string sender):
  Message(DEBUG_METRICS, std::move(sender)),
  bytesFromStore_(bytesFromStore) {}

std::string DebugMetricsMessage::getTypeString() const {
  return "DebugMetricsMessage";
}

int64_t DebugMetricsMessage::getBytesFromStore() const {
  return bytesFromStore_;
}

}
