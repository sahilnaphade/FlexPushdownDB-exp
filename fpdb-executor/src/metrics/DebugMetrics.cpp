//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

const TransferMetrics &DebugMetrics::getTransferMetrics() const {
  return transferMetrics_;
}

int DebugMetrics::getNumPushdownFallBack() const {
  return numPushdownFallBack_;
}

void DebugMetrics::add(const TransferMetrics &transferMetrics) {
  transferMetrics_.add(transferMetrics);
}

void DebugMetrics::incPushdownFallBack() {
  ++numPushdownFallBack_;
}

}
