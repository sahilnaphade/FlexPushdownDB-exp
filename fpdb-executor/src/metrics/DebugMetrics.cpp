//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

DebugMetrics::DebugMetrics():
  updateMutex_(std::make_shared<std::mutex>()) {}

int64_t DebugMetrics::getBytesFromStore() const {
  return bytesFromStore_;
}

void DebugMetrics::addBytesFromStore(int64_t bytes) {
  std::lock_guard<std::mutex> guard(*updateMutex_);
  bytesFromStore_ += bytes;
}

}
