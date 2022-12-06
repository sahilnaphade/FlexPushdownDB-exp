//
// Created by Yifei Yang on 12/6/22.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H

#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <unordered_map>
#include <string>

namespace fpdb::main::test {

class BitmapPushdownTestUtil {

public:
  static void run_bitmap_pushdown_benchmark_query(const std::string &cachingQuery,
                                                  const std::string &testQuery,
                                                  bool isSsb,
                                                  const std::string &sf,
                                                  int parallelDegree,
                                                  bool startFPDBStore);

private:
  static void set_pushdown_flags(std::unordered_map<std::string, bool> &flags);
  static void reset_pushdown_flags(std::unordered_map<std::string, bool> &flags);

  static void startFPDBStoreServer();
  static void stopFPDBStoreServer();

  static std::shared_ptr<fpdb::store::server::Server> fpdbStoreServer_;
  static std::shared_ptr<fpdb::store::server::caf::ActorManager> actorManager_;
  static std::shared_ptr<fpdb::store::client::FPDBStoreClientConfig> fpdbStoreClientConfig_;
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_BITMAPPUSHDOWNTESTUTIL_H
