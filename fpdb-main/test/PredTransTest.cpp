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

TEST_SUITE ("pred-trans" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-03" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql");
}

TEST_CASE ("pred-trans-tpch-05" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql");
}

TEST_CASE ("pred-trans-tpch-19" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql");
}

}

}
