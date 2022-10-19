//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/message/DebugMetricsMessage.h>

namespace fpdb::executor::message {

DebugMetricsMessage::DebugMetricsMessage(const executor::metrics::DebugMetrics &debugMetrics,
                                         const std::string &sender):
  Message(DEBUG_METRICS, sender),
  debugMetrics_(debugMetrics) {}

std::string DebugMetricsMessage::getTypeString() const {
  return "DebugMetricsMessage";
}

const executor::metrics::DebugMetrics &DebugMetricsMessage::getDebugMetrics() const {
  return debugMetrics_;
}

}
