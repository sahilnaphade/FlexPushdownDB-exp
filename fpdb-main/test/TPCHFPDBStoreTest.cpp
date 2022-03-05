//
// Created by Yifei Yang on 3/4/22.
//

#include <doctest/doctest.h>
#include <fpdb/store/server/Server.hpp>
#include "TestUtil.h"

namespace fpdb::main::test {

#define SKIP_SUITE false

std::shared_ptr<fpdb::store::server::Server> fpdbStoreServer;
std::shared_ptr<fpdb::store::server::caf::ActorManager> actorManager;

void startFPDBStoreServer() {
  actorManager = fpdb::store::server::caf::ActorManager::make<::caf::id_block::Server>().value();
  fpdbStoreServer = fpdb::store::server::Server::make(fpdb::store::server::ServerConfig{"1",
                                                                                        0,
                                                                                        true,
                                                                                        std::nullopt,
                                                                                        0,
                                                                                        TestUtil::FlightPort,
                                                                                        TestUtil::FileServicePort,
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

TEST_SUITE ("tpch-sf0.01-fpdb-store-pullup" * doctest::skip(SKIP_SUITE)) {

int parallelDegree = 3;

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  stopFPDBStoreServer();
}

}

TEST_SUITE ("tpch-sf0.01-fpdb-store-pushdown-only" * doctest::skip(SKIP_SUITE)) {

int parallelDegree = 3;

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

TEST_CASE ("tpch-sf0.01-fpdb-store-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  stopFPDBStoreServer();
}

}

}
