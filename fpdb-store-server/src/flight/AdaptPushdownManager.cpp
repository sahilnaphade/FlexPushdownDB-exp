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

tl::expected<bool, std::string> AdaptPushdownManager::receiveOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);

  // find adaptive pushdown metrics
  auto adaptPushdownMetricsKeys = generateAdaptPushdownMetricsKeys(req->queryId_, req->op_);
  auto pullupMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->first);
  auto pushdownMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->second);
  if (pullupMetricsIt == adaptPushdownMetrics_.end()) {
    return tl::make_unexpected(fmt::format("Pullup metrics of req '{}-{}' not found", req->queryId_, req->op_));
  }
  if (pushdownMetricsIt == adaptPushdownMetrics_.end()) {
    return tl::make_unexpected(fmt::format("Pushdown metrics of req '{}-{}' not found", req->queryId_, req->op_));
  }

  // check if need to fall back to pullup
  int64_t pullupTime = pullupMetricsIt->second;
  req->estExecTime_ = pushdownMetricsIt->second;
  auto expWaitTime = getWaitTime(req);
  if (!expWaitTime.has_value()) {
    return tl::make_unexpected(expWaitTime.error());
  }
  // need to calibrate by multiplying a constant, since
  //  1) Pullup metrics are measured when not detached, but in real runs they are detached;
  //  2) Fall back actually incurs double requests, which incurs additional overhead.
  return pullupTime * 3 > *req->estExecTime_ + *expWaitTime;
}

void AdaptPushdownManager::admitOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);
  reqSet_.emplace(req);
  reqManageCv_.wait(lock, [&] {
    return numUsedThreads_ < MaxThreads;
  });
  req->startTime_ = std::chrono::steady_clock::now();
  numUsedThreads_ += req->numRequiredCpuCores_;
}

void AdaptPushdownManager::finishOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(reqManageMutex_);
  reqSet_.erase(req);
  numUsedThreads_ -= req->numRequiredCpuCores_;
  reqManageCv_.notify_one();
}

tl::expected<int64_t, std::string> AdaptPushdownManager::getWaitTime(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  int64_t waitTime = 0;
  auto currTime = std::chrono::steady_clock::now();
  for (const auto &existReq: reqSet_) {
    if (!existReq->estExecTime_.has_value()) {
      return tl::make_unexpected(fmt::format("Estimated execution time of req '{}-{}' not set",
                                             existReq->queryId_, existReq->op_));
    }
    if (existReq->startTime_.has_value()) {
      waitTime += std::max((int64_t) 0, (int64_t) (*existReq->estExecTime_ -
          std::chrono::duration_cast<std::chrono::nanoseconds>(currTime - *existReq->startTime_).count()));
    } else {
      waitTime += *existReq->estExecTime_;
    }
  }
  return (waitTime * 1.0 / MaxThreads) * (req->numRequiredCpuCores_ * 1.0 / MaxThreads);
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
