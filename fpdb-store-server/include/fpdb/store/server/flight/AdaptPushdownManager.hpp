//
// Created by Yifei Yang on 10/31/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP

#include <unordered_map>
#include <vector>
#include <string>

namespace fpdb::store::server::flight {

class AdaptPushdownManager {

public:
  AdaptPushdownManager() = default;

  void addAdaptPushdownMetrics(const std::unordered_map<std::string, int64_t> &other);

private:
  std::unordered_map<std::string, int64_t> adaptPushdownMetrics_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_ADAPTPUSHDOWNMANAGER_HPP
