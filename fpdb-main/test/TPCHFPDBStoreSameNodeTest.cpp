//
// Created by Yifei Yang on 3/4/22.
//

#include <doctest/doctest.h>
#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include "TestUtil.h"
#include "Globals.h"

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

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

}

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-csv-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

}

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

}

TEST_SUITE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-same-node-parquet-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

}

}
