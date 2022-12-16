//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

const TransferMetrics &DebugMetrics::getTransferMetrics() const {
  return transferMetrics_;
}

void DebugMetrics::add(const TransferMetrics &transferMetrics) {
  std::lock_guard<std::mutex> guard(updateMutex_);
  transferMetrics_.add(transferMetrics);
}

void DebugMetrics::incPushdownFallBack() {
  std::lock_guard<std::mutex> guard(updateMutex_);
  ++numPushdownFallback_;
}

}
