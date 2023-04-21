//
// Created by Yifei Yang on 4/20/23.
//

#include <fpdb/executor/metrics/PredTransMetrics.h>

namespace fpdb::executor::metrics {

const std::unordered_set<PredTransMetrics::PTMetricsUnit,
                         PredTransMetrics::PTMetricsUnitHash,
                         PredTransMetrics::PTMetricsUnitPred> &PredTransMetrics::getMetrics() const {
  return metrics_;
}

void PredTransMetrics::add(const PTMetricsUnit &unit) {
  auto it = metrics_.find(unit);
  if (it == metrics_.end()) {
    metrics_.emplace(unit);
  } else {
    it->numRows_ += unit.numRows_;
  }
}

}
