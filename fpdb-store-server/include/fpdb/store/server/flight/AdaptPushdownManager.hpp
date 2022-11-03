//
// Created by Yifei Yang on 10/31/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP

#include <tl/expected.hpp>
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <semaphore>
#include <queue>

namespace fpdb::store::server::flight {

struct AdaptPushdownReqInfo {
  long queryId_;
  std::string op_;
  int numRequiredCpuCores_;
  std::shared_ptr<std::binary_semaphore> sem_;
};

class AdaptPushdownManager {

public:
  AdaptPushdownManager() = default;

  // Save adaptive pushdown metrics
  void addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other);

  // process an incoming pushdown request, return "true" to execute as pushdown, "false" to fall back as pullup
  bool receiveOne(const AdaptPushdownReqInfo &req);

  // when, one request is finished, check if we need to pop out requests from wait queue
  void finishOne(int numReleasedCpuCores);

private:
  // admit to execute a pushdown request
  void admit(const AdaptPushdownReqInfo &req);

  // get the waiting time at this point
  int64_t getCurrWaitTime();

  // if the incoming pushdown request needs to wait
  bool wait(const AdaptPushdownReqInfo &req);

  // generate adaptive pushdown metrics keys for both pullup metrics and pushdown metrics
  static tl::expected<std::pair<std::string, std::string>, std::string>
  generateAdaptPushdownMetricsKeys(long queryId, const std::string &op);

  std::unordered_map<std::string, int64_t> adaptPushdownMetrics_;
  std::queue<AdaptPushdownReqInfo> waitQueue_;
  std::mutex adaptPushdownMetricsMutex_, waitQueueMutex_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
