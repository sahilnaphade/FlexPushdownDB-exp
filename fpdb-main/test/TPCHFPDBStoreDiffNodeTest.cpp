//
// Created by Yifei Yang on 3/4/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"

/**
 * Start Calcite server and FPDB store server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-pullup" * doctest::skip(SKIP_SUITE)) {

int parallelDegreeFPDBStoreDiffNode = 23;

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-pushdown-only" * doctest::skip(SKIP_SUITE)) {

int parallelDegreeFPDBStoreDiffNode = 23;

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegreeFPDBStoreDiffNode,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

}
