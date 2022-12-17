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
                                                               const std::vector<int> &availCpuPercents,
                                                               int parallelDegree,
                                                               bool startFPDBStore) {
  bool oldEnableAdaptPushdown;
  int oldAvailCpuPercent;
  if (startFPDBStore) {
    TestUtil::startFPDBStoreServer();
  }

  // adaptive pushdown metrics runs
  // adaptive pushdown metrics of pullup run
  TestUtil testUtil(schemaName,
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

  for (int availCpuPercent: availCpuPercents) {
    std::cout << fmt::format("Available CPU: {}%\n", availCpuPercent) << std::endl;
    // pullup baseline run
    std::cout << fmt::format("Pullup baseline run", availCpuPercent) << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pullupMode());
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, availCpuPercent, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);

    // pushdown baseline run
    std::cout << fmt::format("Pushdown baseline run", availCpuPercent) << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, false, availCpuPercent, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);

    // adaptive pushdown test run
    std::cout << fmt::format("Adaptive pushdown run", availCpuPercent) << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    set_pushdown_flags(&oldEnableAdaptPushdown, &oldAvailCpuPercent, true, availCpuPercent, !startFPDBStore);
    REQUIRE_NOTHROW(testUtil.runTest());
    reset_pushdown_flags(oldEnableAdaptPushdown, oldAvailCpuPercent, !startFPDBStore);
  }

  if (startFPDBStore) {
    TestUtil::stopFPDBStoreServer();
  } else {
    // if fpdb-store is remote, need to clear adaptive pushdown metrics
    send_cmd_to_storage(fpdb::store::server::flight::ClearAdaptPushdownMetricsCmd::make());
  }
}

void AdaptPushdownTestUtil::set_pushdown_flags(bool *oldEnableAdaptPushdown, int* oldAvailCpuPercent,
                                               bool enableAdaptPushdown, int availCpuPercent,
                                               bool isFPDBStoreRemote) {
  *oldEnableAdaptPushdown = fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN;
  *oldAvailCpuPercent = fpdb::store::server::flight::AvailCpuPercent;
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = enableAdaptPushdown;
  if (isFPDBStoreRemote) {
    send_cmd_to_storage(fpdb::store::server::flight::SetAdaptPushdownCmd::make(enableAdaptPushdown));
  }
  fpdb::store::server::flight::AvailCpuPercent = availCpuPercent;
}

void AdaptPushdownTestUtil::reset_pushdown_flags(bool oldEnableAdaptPushdown, int oldAvailCpuPercent,
                                                 bool isFPDBStoreRemote) {
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = oldEnableAdaptPushdown;
  if (isFPDBStoreRemote) {
    send_cmd_to_storage(fpdb::store::server::flight::SetAdaptPushdownCmd::make(oldEnableAdaptPushdown));
  }
  fpdb::store::server::flight::AvailCpuPercent = oldAvailCpuPercent;
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
