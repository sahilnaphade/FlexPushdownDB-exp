//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H

#include <fpdb/executor/metrics/Globals.h>
#include <mutex>

namespace fpdb::executor::metrics {

class DebugMetrics {

public:
  DebugMetrics(int64_t bytesFromStore,
               int64_t bytesToStore,
               int64_t bytesInterCompute);
  DebugMetrics();
  DebugMetrics(const DebugMetrics&) = default;
  DebugMetrics& operator=(const DebugMetrics&) = default;
  ~DebugMetrics() = default;

  int64_t getBytesFromStore() const;
  int64_t getBytesToStore() const;
  int64_t getBytesInterCompute() const;
  void initUpdate();
  void add(const DebugMetrics &other);

private:
  int64_t bytesFromStore_;
  int64_t bytesToStore_;
  int64_t bytesInterCompute_;
  std::shared_ptr<std::mutex> updateMutex_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DebugMetrics& metrics) {
    return f.object(metrics).fields(f.field("bytesFromStore", metrics.bytesFromStore_),
                                    f.field("bytesToStore", metrics.bytesToStore_),
                                    f.field("bytesInterCompute", metrics.bytesInterCompute_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
