//
// Created by Yifei Yang on 3/4/22.
//

#include <doctest/doctest.h>
#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include "TestUtil.h"

/**
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

std::shared_ptr<fpdb::store::server::Server> fpdbStoreServer;
std::shared_ptr<fpdb::store::server::caf::ActorManager> actorManager;
std::shared_ptr<fpdb::store::client::FPDBStoreClientConfig> fpdbStoreClientConfig =
        fpdb::store::client::FPDBStoreClientConfig::parseFPDBStoreClientConfig();

void startFPDBStoreServer() {
  actorManager = fpdb::store::server::caf::ActorManager::make<::caf::id_block::Server>().value();
  fpdbStoreServer = fpdb::store::server::Server::make(
          fpdb::store::server::ServerConfig{"1",
                                            0,
                                            true,
                                            std::nullopt,
                                            0,
                                            fpdbStoreClientConfig->getFlightPort(),
                                            fpdbStoreClientConfig->getFileServicePort(),
                                            "test-resources"},
          std::nullopt,
          actorManager);
  auto initResult = fpdbStoreServer->init();
  REQUIRE(initResult.has_value());
  auto startResult = fpdbStoreServer->start();
  REQUIRE(startResult.has_value());
}

void stopFPDBStoreServer() {
  fpdbStoreServer->stop();
  fpdbStoreServer.reset();
  actorManager.reset();
}

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-pullup" * doctest::skip(SKIP_SUITE)) {

int parallelDegreeFPDBStoreSameNode = 3;

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

}

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only" * doctest::skip(SKIP_SUITE)) {

int parallelDegreeFPDBStoreSameNode = 3;

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegreeFPDBStoreSameNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

}

}
