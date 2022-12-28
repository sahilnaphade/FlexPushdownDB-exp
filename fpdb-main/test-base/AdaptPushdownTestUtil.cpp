//
// Created by Yifei Yang on 12/13/22.
//

#include "AdaptPushdownTestUtil.h"
#include "TestUtil.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/store/server/flight/Util.hpp>
#include <fpdb/store/server/flight/ClearAdaptPushdownMetricsCmd.hpp>
#include <fpdb/store/server/flight/SetAdaptPushdownCmd.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <arrow/flight/api.h>
#include <doctest/doctest.h>

namespace fpdb::main::test {

void AdaptPushdownTestUtil::run_adapt_pushdown_benchmark_query(const std::string &schemaName,
                                                               const std::string &queryFileName,
                                                               const std::vector<int> &maxThreadsVec,
                                                               int parallelDegree,
                                                               bool startFPDBStore) {
  bool oldEnableAdaptPushdown;
  int oldAvailCpuPercent;
  if (startFPDBStore) {
    TestUtil::startFPDBStoreServer();
  }

  // run pullup and pushdown once as gandiva cache makes the subsequent runs faster than the first run
  std::cout << "Start run (pullup)" << std::endl;
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pullupMode()));
  std::cout << "Start run (pushdown)" << std::endl;
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));

  for (int maxThreads: maxThreadsVec) {
    std::cout << fmt::format("Max threads at storage side: {}\n", maxThreads) << std::endl;

    // pullup baseline run, also collecting adaptive pushdown metrics
    std::cout << "Pullup baseline run" << std::endl;
    TestUtil testUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pullupMode());
    testUtil.setCollAdaptPushdownMetrics(true);
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, maxThreads, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);

    // pushdown baseline run, also collecting adaptive pushdown metrics.
    std::cout << "Pushdown baseline run" << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    testUtil.setCollAdaptPushdownMetrics(true);
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, maxThreads, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);

    // adaptive pushdown test run
    std::cout << "Adaptive pushdown run" << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, true, maxThreads, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);

    // clear adaptive pushdown metrics
    send_cmd_to_storage(fpdb::store::server::flight::ClearAdaptPushdownMetricsCmd::make());
  }

  if (startFPDBStore) {
    TestUtil::stopFPDBStoreServer();
  }
}

void AdaptPushdownTestUtil::set_pushdown_flags(bool *oldEnableAdaptPushdown, int* oldMaxThreads,
                                               bool enableAdaptPushdown, int maxThreads,
                                               bool isFPDBStoreRemote) {
  *oldEnableAdaptPushdown = fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN;
  *oldMaxThreads = fpdb::store::server::flight::MaxThreads;
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = enableAdaptPushdown;
  if (isFPDBStoreRemote) {
    send_cmd_to_storage(fpdb::store::server::flight::SetAdaptPushdownCmd::make(
            enableAdaptPushdown, maxThreads));
  }
  fpdb::store::server::flight::MaxThreads = maxThreads;
}

void AdaptPushdownTestUtil::reset_pushdown_flags(bool oldEnableAdaptPushdown, int oldMaxThreads,
                                                 bool isFPDBStoreRemote) {
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = oldEnableAdaptPushdown;
  if (isFPDBStoreRemote) {
    send_cmd_to_storage(fpdb::store::server::flight::SetAdaptPushdownCmd::make(
            oldEnableAdaptPushdown, oldMaxThreads));
  }
  fpdb::store::server::flight::MaxThreads = oldMaxThreads;
}

void AdaptPushdownTestUtil::send_cmd_to_storage(const std::shared_ptr<fpdb::store::server::flight::CmdObject> &cmdObj) {
  auto expCmd = cmdObj->serialize(false);
  if (!expCmd.has_value()) {
    throw std::runtime_error(expCmd.error());
    return;
  }

  // send to each fpdb-store node
  auto fpdbStoreClientConfig = fpdb::store::client::FPDBStoreClientConfig::parseFPDBStoreClientConfig();
  for (const auto &host: fpdbStoreClientConfig->getHosts()) {
    auto client = flight::GlobalFlightClients.getFlightClient(host, fpdbStoreClientConfig->getFlightPort());
    auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
    auto status = client->DoPut(descriptor, nullptr, &writer, &metadataReader);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
      return;
    }
    status = writer->Close();
    if (!status.ok()) {
      throw std::runtime_error(status.message());
      return;
    }
  }
}

}
