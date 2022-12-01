//
// Created by Yifei Yang on 3/4/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (single compute node, single FPDB store node)
 *
 * Start Calcite server and FPDB store server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-csv-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-csv-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-diff-node-parquet-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_FPDB_STORE_DIFF_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}


}
