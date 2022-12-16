//
// Created by Yifei Yang on 10/31/22.
//

#include "fpdb/store/server/flight/AdaptPushdownManager.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fpdb/executor/physical/Globals.h"
#include "fpdb/util/Util.h"
#include "thread"

namespace fpdb::store::server::flight {

void AdaptPushdownManager::addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other) {
  std::unique_lock lock(adaptPushdownMetricsMutex_);
  adaptPushdownMetrics_.insert(other.begin(), other.end());
}

void AdaptPushdownManager::clearAdaptPushdownMetrics() {
  std::unique_lock lock(adaptPushdownMetricsMutex_);
  adaptPushdownMetrics_.clear();
}

bool AdaptPushdownManager::receiveOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(waitQueueMutex_);

  // check if need to fall back to pullup
  auto adaptPushdownMetricsKeys = generateAdaptPushdownMetricsKeys(req->queryId_, req->op_);
  auto pullupMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->first);
  auto pushdownMetricsIt = adaptPushdownMetrics_.find(adaptPushdownMetricsKeys->second);
  bool execAsPushdown;
  std::optional<int64_t> estExecTime = std::nullopt;
  if (pullupMetricsIt == adaptPushdownMetrics_.end() || pushdownMetricsIt == adaptPushdownMetrics_.end()) {
    execAsPushdown = true;
  } else {
    int64_t pullupTime = pullupMetricsIt->second;
    int64_t pushdownTime = pushdownMetricsIt->second;
    int64_t waitTime = getWaitTime();
    execAsPushdown = (pullupTime >= pushdownTime + waitTime);
    estExecTime = pushdownTime;
  }

  // check if need to wait if we execute it as pushdown
  if (execAsPushdown) {
    req->estExecTime_ = estExecTime;
    if (wait()) {
      waitQueue_.push(req);
      estExecTimeInWaitQueue_ += estExecTime.value_or(0);
    } else {
      admit(req);
    }
  }

  return execAsPushdown;
}

void AdaptPushdownManager::finishOne(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  std::unique_lock lock(waitQueueMutex_);

  // set status and remove from execution set
  req->status_ = AdaptPushdownReqInfo::STATUS::FINISH;
  execSet_.erase(req);

  // skip if no request is waiting
  if (waitQueue_.empty()) {
    return;
  }

  // if no request is being executed, we should at least pop one request from wait queue
  if (execSet_.empty()) {
    const auto &front = waitQueue_.front();
    admit(front);
    waitQueue_.pop();
    estExecTimeInWaitQueue_ -= front->estExecTime_.value_or(0);
  }

  // then regular case, which checks CPU usage to determine
  double cpuUsage = fpdb::util::getCpuUsage();
  while (cpuUsage < AvailCpuPercent && !waitQueue_.empty()) {
    const auto &front = waitQueue_.front();
    admit(front);
    waitQueue_.pop();
    estExecTimeInWaitQueue_ -= front->estExecTime_.value_or(0);
    cpuUsage += front->numRequiredCpuCores_ / ((double) std::thread::hardware_concurrency()) * 100.0;
  }
}

void AdaptPushdownManager::admit(const std::shared_ptr<AdaptPushdownReqInfo> &req) {
  req->mutex_->lock();
  req->status_ = AdaptPushdownReqInfo::STATUS::RUN;
  req->cv_->notify_one();
  req->mutex_->unlock();
  req->startTime_ = std::chrono::steady_clock::now();
  execSet_.emplace(req);
}

int64_t AdaptPushdownManager::getWaitTime() {
  int64_t waitTime = estExecTimeInWaitQueue_;
  auto currTime = std::chrono::steady_clock::now();
  for (const auto &execReq: execSet_) {
    if (execReq->estExecTime_.has_value()) {
      waitTime += std::max((int64_t) 0, (int64_t) (*execReq->estExecTime_ -
          std::chrono::duration_cast<chrono::nanoseconds>(currTime - *execReq->startTime_).count()));
    }
  }
  return waitTime / (std::thread::hardware_concurrency() * AvailCpuPercent / 100.0);
}

bool AdaptPushdownManager::wait() {
  if (!waitQueue_.empty()) {
    return true;
  }
  return fpdb::util::getCpuUsage() >= AvailCpuPercent;
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
