//
// Created by Yifei Yang on 10/31/22.
//

#include "fpdb/store/server/flight/AdaptPushdownManager.hpp"
#include "fpdb/executor/physical/Globals.h"
#include "fpdb/util/Util.h"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

void AdaptPushdownManager::addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other) {
  std::unique_lock lock(adaptPushdownMetricsMutex_);

  adaptPushdownMetrics_.insert(other.begin(), other.end());
}

bool AdaptPushdownManager::receiveOne(const AdaptPushdownReqInfo &req) {
  std::unique_lock lock(waitQueueMutex_);

  // check if need to fall back to pullup
  bool execAsPushdown;
  auto adaptPushdownMetricsKeys = generateAdaptPushdownMetricsKeys(req.queryId_, req.op_);
  auto pullupMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->first);
  auto pushdownMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->second);
  if (pullupMetricsIt == adaptPushdownMetrics_.end() || pushdownMetricsIt == adaptPushdownMetrics_.end()) {
    execAsPushdown = true;
  } else {
    int64_t pullupTime = pullupMetricsIt->second;
    int64_t pushdownTime = pushdownMetricsIt->second;
    int64_t waitTime = getCurrWaitTime();
    execAsPushdown = (pullupTime >= pushdownTime + waitTime);
  }

  // check if need to wait if we execute it as pushdown
  if (execAsPushdown) {
    if (wait(req)) {
      waitQueue_.push(req);
    } else {
      admit(req);
    }
  }

  return execAsPushdown;
}

void AdaptPushdownManager::finishOne(int numReleasedCpuCores) {
  std::unique_lock lock(waitQueueMutex_);

  while (numReleasedCpuCores > 0 && !waitQueue_.empty()) {
    auto &front = waitQueue_.front();
    int numCpuCoresToConsume = std::min(front.numRequiredCpuCores_, numReleasedCpuCores);
    front.numRequiredCpuCores_ -= numCpuCoresToConsume;
    numReleasedCpuCores -= numCpuCoresToConsume;
    if (front.numRequiredCpuCores_ <= 0) {
      admit(front);
      waitQueue_.pop();
    }
  }
}

void AdaptPushdownManager::admit(const AdaptPushdownReqInfo &req) {
  req.sem_->release();
}

int64_t AdaptPushdownManager::getCurrWaitTime() {
  // TODO
  return 0.0;
}

bool AdaptPushdownManager::wait(const AdaptPushdownReqInfo &req) {
  return fpdb::util::getAvailCpuCores() < req.numRequiredCpuCores_;
}

tl::expected<std::pair<std::string, std::string>, std::string>
AdaptPushdownManager::generateAdaptPushdownMetricsKeys(long queryId, const std::string &op) {
  if (op.substr(0, fpdb::executor::physical::PushdownOpNamePrefix.size()) ==
      fpdb::executor::physical::PushdownOpNamePrefix) {
    auto pos = op.find("]");
    auto pushdownMetricsKey = fmt::format("{}-{}", queryId, op.substr(0, pos + 1));
    std::string pullupMetricsKey = pushdownMetricsKey;
    pullupMetricsKey.replace(pullupMetricsKey.find(fpdb::executor::physical::PushdownOpNamePrefix),
                             fpdb::executor::physical::PushdownOpNamePrefix.size(),
                             fpdb::executor::physical::PullupOpNamePrefix);
    return std::make_pair(pullupMetricsKey, pushdownMetricsKey);
  } else {
    return tl::make_unexpected(fmt::format("Invalid op name for adaptive pushdown metrics: {}", op));
  }
}

}
