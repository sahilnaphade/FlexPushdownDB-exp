//
// Created by Yifei Yang on 4/12/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/util/Util.h>
#include <cstdio>

using namespace fpdb::util;

namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("bitmap-pushdown-bench" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-bench-tpch-sf50-fpdb-store-diff-node-compute-bitmap" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;

  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileNameBase = "test_{}.sql";

  // predicates with "and" that makes ~20% selectivity
  std::vector<std::string> allPredicates{"l_discount <= 0.01",
                                         "l_quantity >= 0",
                                         "l_shipdate <= date '1998-12-31'",
                                         "l_commitdate <= date '1998-12-31'",
                                         "l_receiptdate <= date '1998-12-31'",
                                         "l_orderkey >= 0",
                                         "l_partkey >= 0"};

  for (uint predicateNum = 1; predicateNum <= allPredicates.size(); ++predicateNum) {
    // create queries
    std::vector<std::string> predicates(allPredicates.begin(), allPredicates.begin() + predicateNum);
    std::string cachingQuery = fmt::format("select\n"
                                           "    l_discount\n"
                                           "from\n"
                                           "    lineitem\n"
                                           "where\n"
                                           "    {}\n", fmt::join(predicates, " and "));
    std::string testQuery = fmt::format("select\n"
                                        "    l_tax, l_extendedprice\n"
                                        "from\n"
                                        "    lineitem\n"
                                        "where\n"
                                        "    {}\n", fmt::join(predicates, " and "));
    std::string testQueryFileName = fmt::format(testQueryFileNameBase, predicateNum);
    TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
    TestUtil::writeQueryToFile(testQueryFileName, testQuery);

    // run test
    TestUtil testUtil("tpch-sf50/parquet/",
                      {cachingQueryFileName,
                       testQueryFileName},
                      PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::hybridMode(),
                      CachingPolicyType::LFU,
                      50L * 1024 * 1024 * 1024);
    // fix cache layout after caching query, otherwise it keeps fetching new segments
    // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
    // pushdown part actually does nothing)
    testUtil.setFixLayoutIndices({0});
    REQUIRE_NOTHROW(testUtil.runTest());

    // delete queries
    TestUtil::removeQueryFile(cachingQueryFileName);
    TestUtil::removeQueryFile(testQueryFileName);
  }
}

TEST_CASE ("bitmap-pushdown-bench-tpch-sf50-fpdb-store-diff-node-storage-bitmap" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;

  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileNameBase = "test_{}.sql";

  // predicates with "and" that makes ~20% selectivity
  std::vector<std::string> projectColumns{"l_returnflag",
                                          "l_linestatus",
                                          "l_quantity",
                                          "l_extendedprice",
                                          "l_tax",
                                          "l_shipdate",
                                          "l_commitdate"};

  for (uint projectColumnNum = 1; projectColumnNum <= projectColumns.size(); ++projectColumnNum) {
    // create queries, test query with ~20% selectivity
    std::vector<std::string> cachedProjectColumns(projectColumns.begin(), projectColumns.begin() + projectColumnNum);
    std::string cachingQuery = fmt::format("select\n"
                                           "    {}\n"
                                           "from\n"
                                           "    lineitem\n"
                                           "where\n"
                                           "    l_quantity < 20\n", fmt::join(cachedProjectColumns, ", "));
    std::string testQuery = fmt::format("select\n"
                                        "    {}\n"
                                        "from\n"
                                        "    lineitem\n"
                                        "where\n"
                                        "    l_discount <= 0.01\n", fmt::join(cachedProjectColumns, ", "));
    std::string testQueryFileName = fmt::format(testQueryFileNameBase, projectColumnNum);
    TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
    TestUtil::writeQueryToFile(testQueryFileName, testQuery);

    // run test
    TestUtil testUtil("tpch-sf50/parquet/",
                      {cachingQueryFileName,
                       testQueryFileName},
                      PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::hybridMode(),
                      CachingPolicyType::LFU,
                      50L * 1024 * 1024 * 1024);
    // fix cache layout after caching query, otherwise it keeps fetching new segments
    // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
    // pushdown part actually does nothing)
    testUtil.setFixLayoutIndices({0});
    REQUIRE_NOTHROW(testUtil.runTest());

    // delete queries
    TestUtil::removeQueryFile(cachingQueryFileName);
    TestUtil::removeQueryFile(testQueryFileName);
  }
}

}

}
