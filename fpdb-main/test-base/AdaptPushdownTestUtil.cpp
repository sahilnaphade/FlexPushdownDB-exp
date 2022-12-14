//
// Created by Yifei Yang on 12/13/22.
//

#include "AdaptPushdownTestUtil.h"
#include "TestUtil.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/store/server/flight/Util.hpp>
#include <doctest/doctest.h>

namespace fpdb::main::test {

void AdaptPushdownTestUtil::run_adapt_pushdown_benchmark_query(const std::string &schemaName,
                                                               const std::string &queryFileName,
                                                               int availCpuPercent,
                                                               int parallelDegree,
                                                               bool startFPDBStore) {
  bool oldEnableAdaptPushdown;
  int oldAvailCpuPercent;
  if (startFPDBStore) {
    TestUtil::startFPDBStoreServer();
  }

  // pullup baseline run
  TestUtil testUtil(schemaName,
                    {queryFileName},
                    parallelDegree,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::pullupMode());
  set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, availCpuPercent);
  REQUIRE_NOTHROW(testUtil.runTest());
  reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent);

  // pushdown baseline run
  testUtil = TestUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pushdownOnlyMode());
  set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, availCpuPercent);
  REQUIRE_NOTHROW(testUtil.runTest());
  reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent);

  // adaptive pushdown run
  // adaptive pushdown metrics of pullup run
  testUtil = TestUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pullupMode());
  testUtil.setCollAdaptPushdownMetrics(true);
  REQUIRE_NOTHROW(testUtil.runTest());

  // adaptive pushdown metrics of pushdown run
  testUtil = TestUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pushdownOnlyMode());
  testUtil.setCollAdaptPushdownMetrics(true);
  REQUIRE_NOTHROW(testUtil.runTest());

  // adaptive pushdown test run
  testUtil = TestUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pushdownOnlyMode());
  set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, true, availCpuPercent);
  REQUIRE_NOTHROW(testUtil.runTest());
  reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent);

  if (startFPDBStore) {
    TestUtil::stopFPDBStoreServer();
  }
}

void AdaptPushdownTestUtil::set_pushdown_flags(bool *oldEnableAdaptPushdown, int* oldAvailCpuPercent,
                                               bool enableAdaptPushdown, int availCpuPercent) {
  *oldEnableAdaptPushdown = fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN;
  *oldAvailCpuPercent = fpdb::store::server::flight::AvailCpuPercent;
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = enableAdaptPushdown;
  fpdb::store::server::flight::AvailCpuPercent = availCpuPercent;
}

void AdaptPushdownTestUtil::reset_pushdown_flags(bool oldEnableAdaptPushdown, int oldAvailCpuPercent) {
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = oldEnableAdaptPushdown;
  fpdb::store::server::flight::AvailCpuPercent = oldAvailCpuPercent;
}

}
