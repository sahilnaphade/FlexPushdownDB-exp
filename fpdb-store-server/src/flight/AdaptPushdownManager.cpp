//
// Created by Yifei Yang on 10/31/22.
//

#include "fpdb/store/server/flight/AdaptPushdownManager.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fpdb/executor/physical/Globals.h"
#include "fpdb/util/CPUMonitor.h"
#include "thread"

namespace fpdb::store::server::flight {

void AdaptPushdownManager::addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other) {
  std::unique_lock lock(metricsMutex_);
  adaptPushdownMetrics_.insert(other.begin(), other.end());
}

void AdaptPushdownManager::clearAdaptPushdownMetrics() {
  std::unique_lock lock(metricsMutex_);
  adaptPushdownMetrics_.clear();
}

bool AdaptPushdownManager::receiveOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);

  // check if need to fall back to pullup
  auto adaptPushdownMetricsKeys = generateAdaptPushdownMetricsKeys(req->queryId_, req->op_);
  auto pullupMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->first);
  auto pushdownMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->second);
  bool execAsPushdown;
  if (pullupMetricsIt == adaptPushdownMetrics_.end() || pushdownMetricsIt == adaptPushdownMetrics_.end()) {
    execAsPushdown = true;
  } else {
    int64_t pullupTime = pullupMetricsIt->second;
    req->estExecTime_ = pushdownMetricsIt->second;
    int64_t waitExecTime = getWaitExecTime(req);
    // some calibration needed when observing sample test results
    execAsPushdown = (((double) pullupTime) / ((double) *req->estExecTime_) < 2) ?
            (pullupTime * 1.5 >= waitExecTime) : (pullupTime / 1.5 >= waitExecTime);
  }

  return true;
}

void AdaptPushdownManager::admitOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);
  reqSet_.emplace(req);
  reqManageCv_.wait(lock, [&] {
    return numRunningReqs_ < MaxThreads;
  });
  req->startTime_ = std::chrono::steady_clock::now();
  numRunningReqs_ += req->numRequiredCpuCores_;
}

void AdaptPushdownManager::finishOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);
  reqSet_.erase(req);
  numRunningReqs_ -= req->numRequiredCpuCores_;
  reqManageCv_.notify_one();
}

int64_t AdaptPushdownManager::getWaitExecTime(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  int64_t waitTime = 0;
  auto currTime = std::chrono::steady_clock::now();
  for (const auto &execReq: reqSet_) {
    if (execReq->estExecTime_.has_value()) {
      waitTime += std::max((int64_t) 0, (int64_t) (*execReq->estExecTime_ -
          std::chrono::duration_cast<std::chrono::nanoseconds>(currTime - *execReq->startTime_).count()));
    }
  }
  return waitTime / MaxThreads + *req->estExecTime_;
}

tl::expected<std::pair<std::string, std::string>, std::string>
AdaptPushdownManager::generateAdaptPushdownMetricsKeys(long queryId, const std::string &op) {
  if (op.substr(0, fpdb::executor::physical::PushdownOpNamePrefix.size()) ==
      fpdb::executor::physical::PushdownOpNamePrefix) {
    auto pushdownMetricsKey = fmt::format("{}-{}", queryId, op);
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
