//
// Created by Yifei Yang on 4/11/23.
//

#include <doctest/doctest.h>
#include <fpdb/executor/physical/file/LocalFileScanPOp.h>
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
  fpdb::plan::ENABLE_PRED_TRANS = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = OLD_ENABLE_PRED_TRANS;
}

TEST_SUITE ("pred-trans" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-q03" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01/parquet/", "tpch/original/03.sql");
}

TEST_CASE ("pred-trans-tpch-q05" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01/parquet/", "tpch/original/05.sql");
}

TEST_CASE ("pred-trans-tpch-q19" * doctest::skip(false || SKIP_SUITE)) {
  testPredTrans("tpch-sf0.01/parquet/", "tpch/original/19.sql");
}

}

}
