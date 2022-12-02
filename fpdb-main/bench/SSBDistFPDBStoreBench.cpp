//
// Created by Yifei Yang on 12/1/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * SSB test (multiple compute nodes, FPDB store)
 *
 * Start Calcite server on the coordinator (locally), CAF server on all executors (remote nodes),
 * and FPDB store server before running this (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.4" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.4.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/1.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/2.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.4" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/3.4.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.1.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.2.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-4-node-hash-part/parquet/",
                                            {"ssb/original/4.3.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

}
