//
// Created by Yifei Yang on 10/31/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP

#include "tl/expected.hpp"
#include "fmt/format.h"
#include "unordered_map"
#include "unordered_set"
#include "vector"
#include "string"
#include "mutex"
#include "condition_variable"
#include "queue"

namespace fpdb::store::server::flight {

struct AdaptPushdownReqInfo {
  AdaptPushdownReqInfo(long queryId,
                       const std::string &op,
                       int numRequiredCpuCores,
                       const std::shared_ptr<std::mutex> &mutex,
                       const std::shared_ptr<std::condition_variable_any> &cv):
  queryId_(queryId), op_(op), numRequiredCpuCores_(numRequiredCpuCores),
  mutex_(mutex), cv_(cv) {}

  long queryId_;
  std::string op_;
  int numRequiredCpuCores_;
  std::shared_ptr<std::mutex> mutex_;
  std::shared_ptr<std::condition_variable_any> cv_;
  enum STATUS {WAIT, RUN, FINISH} status_ = STATUS::WAIT;
  std::optional<std::chrono::steady_clock::time_point> startTime_ = std::nullopt;
  std::optional<int64_t> estExecTime_ = std::nullopt;
};

struct AdaptPushdownReqInfoPointerHash {
  inline size_t operator()(const std::shared_ptr<AdaptPushdownReqInfo> &req) const {
    return std::hash<std::string>()(fmt::format("{}-{}", std::to_string(req->queryId_), req->op_));
  }
};

struct AdaptPushdownReqInfoPointerPredicate {
  inline bool operator()(const std::shared_ptr<AdaptPushdownReqInfo> &req1,
                         const std::shared_ptr<AdaptPushdownReqInfo> &req2) const {
    return req1->queryId_ == req2->queryId_ && req1->op_ == req2->op_;
  }
};

class AdaptPushdownManager {

public:
  AdaptPushdownManager() = default;

  // Save adaptive pushdown metrics
  void addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other);

  // Clear adaptive pushdown metrics
  void clearAdaptPushdownMetrics();

  // process an incoming pushdown request, return "true" to execute as pushdown, "false" to fall back as pullup
  bool receiveOne(const std::shared_ptr<AdaptPushdownReqInfo> &req);

  // when, one request is finished, check if we need to pop out requests from wait queue
  void finishOne(const std::shared_ptr<AdaptPushdownReqInfo> &req);

private:
  // admit to execute a pushdown request
  void admit(const std::shared_ptr<AdaptPushdownReqInfo> &req);

  // get the waiting time at this point
  int64_t getWaitTime();

  // if the incoming pushdown request needs to wait
  bool wait();

  // generate adaptive pushdown metrics keys for both pullup metrics and pushdown metrics
  static tl::expected<std::pair<std::string, std::string>, std::string>
  generateAdaptPushdownMetricsKeys(long queryId, const std::string &op);

  std::unordered_map<std::string, int64_t> adaptPushdownMetrics_; // used to estimate pullup and pushdown time
  std::queue<std::shared_ptr<AdaptPushdownReqInfo>> waitQueue_;  // wait queue
  std::unordered_set<std::shared_ptr<AdaptPushdownReqInfo>, AdaptPushdownReqInfoPointerHash,
      AdaptPushdownReqInfoPointerPredicate> execSet_;   // execution set
  int64_t estExecTimeInWaitQueue_ = 0; // total estimated (pushdown) execution time in the wait queue
  std::mutex adaptPushdownMetricsMutex_, waitQueueMutex_;   // used when updating member variables
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
