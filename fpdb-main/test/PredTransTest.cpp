//
// Created by Yifei Yang on 4/11/23.
//

#include <doctest/doctest.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/plan/Globals.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * Predicate transfer test
 * Manually construct query plans for transfer predicates before joins
 *
 * Single compute node, also as the single FPDB store node
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

void testPredTrans(const std::string &schemaName, const std::string &queryFileName) {
  TestUtil::startFPDBStoreServer();
  bool OLD_ENABLE_PRED_TRANS = fpdb::plan::ENABLE_PRED_TRANS;
  bool Old_SHOW_PRED_TRANS_METRICS = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  fpdb::plan::ENABLE_PRED_TRANS = true;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;

  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));

  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = OLD_ENABLE_PRED_TRANS;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = Old_SHOW_PRED_TRANS_METRICS;
}

TEST_SUITE ("pred-trans-tpch-sf0.01-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/01.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/02.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/04.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/06.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/07.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/08.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/09.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/10.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/11.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/12.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/13.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/14.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/15.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/16.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/17.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/18.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/20.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/21.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/22.sql");
}

}

}
