//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

DebugMetrics::DebugMetrics(int64_t bytesFromStore):
  bytesFromStore_(bytesFromStore) {}

void DebugMetrics::initUpdate() {
  updateMutex_ = std::make_shared<std::mutex>();
}

int64_t DebugMetrics::getBytesFromStore() const {
  return bytesFromStore_;
}

void DebugMetrics::add(const DebugMetrics &other) {
  std::lock_guard<std::mutex> guard(*updateMutex_);
  bytesFromStore_ += other.bytesFromStore_;
}

}
