//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

DebugMetrics::DebugMetrics(int64_t bytesFromStore,
                           int64_t bytesToStore,
                           int64_t bytesInterCompute):
  bytesFromStore_(bytesFromStore),
  bytesToStore_(bytesToStore),
  bytesInterCompute_(bytesInterCompute) {}

DebugMetrics::DebugMetrics():
  bytesFromStore_(0),
  bytesToStore_(0),
  bytesInterCompute_(0) {}

void DebugMetrics::initUpdate() {
  updateMutex_ = std::make_shared<std::mutex>();
}

int64_t DebugMetrics::getBytesFromStore() const {
  return bytesFromStore_;
}

int64_t DebugMetrics::getBytesToStore() const {
  return bytesToStore_;
}

int64_t DebugMetrics::getBytesInterCompute() const {
  return bytesInterCompute_;
}

void DebugMetrics::add(const DebugMetrics &other) {
  std::lock_guard<std::mutex> guard(*updateMutex_);
  bytesFromStore_ += other.bytesFromStore_;
  bytesToStore_ += other.bytesToStore_;
  bytesInterCompute_ += other.bytesInterCompute_;
}

}
