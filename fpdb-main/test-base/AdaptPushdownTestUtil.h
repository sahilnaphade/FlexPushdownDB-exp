//
// Created by Yifei Yang on 12/13/22.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H

#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <unordered_map>
#include <string>

namespace fpdb::main::test {

class AdaptPushdownTestUtil {

public:
  static void run_adapt_pushdown_benchmark_query(const std::string &schemaName,
                                                 const std::string &queryFileName,
                                                 const std::vector<int> &availCpuPercents,
                                                 int parallelDegree,
                                                 bool startFPDBStore);

private:
  static void set_pushdown_flags(bool *oldEnableAdaptPushdown, int* oldAvailCpuPercent,
                                 bool enableAdaptPushdown, int availCpuPercent);
  static void reset_pushdown_flags(bool oldEnableAdaptPushdown, int oldAvailCpuPercent);
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H
