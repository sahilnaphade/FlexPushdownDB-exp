//
// Created by Yifei Yang on 5/3/23.
//

#include "PredTransTestUtil.h"
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/plan/Globals.h>
#include <doctest/doctest.h>
#include <limits>

namespace fpdb::main::test {

void PredTransTestUtil::testPredTrans(const std::string &schemaName, const std::string &queryFileName,
                                      bool enablePredTrans) {
  TestUtil::startFPDBStoreServer();
  bool OLD_ENABLE_PRED_TRANS = fpdb::plan::ENABLE_PRED_TRANS;
  bool Old_SHOW_PRED_TRANS_METRICS = fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS;
  fpdb::plan::ENABLE_PRED_TRANS = enablePredTrans;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = true;

  REQUIRE(TestUtil::e2eNoStartCalciteServerSingleThread(schemaName,
                                                        {queryFileName, queryFileName},
                                                        PARALLEL_PRED_TRANS,
                                                        false,
                                                        ObjStoreType::FPDB_STORE,
                                                        Mode::cachingOnlyMode(),
                                                        CachingPolicyType::LFU,
                                                        std::numeric_limits<size_t>::max()));

  TestUtil::stopFPDBStoreServer();
  fpdb::plan::ENABLE_PRED_TRANS = OLD_ENABLE_PRED_TRANS;
  fpdb::executor::metrics::SHOW_PRED_TRANS_METRICS = Old_SHOW_PRED_TRANS_METRICS;
}

}
