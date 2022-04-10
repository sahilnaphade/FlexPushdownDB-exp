//
// Created by Yifei Yang on 4/8/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>

using namespace fpdb::util;

/**
 * Test of bitmap pushdown (hybrid execution), using TPC-H dataset
 * Using the same setup as HybridTest
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

void startFPDBStoreServer();
void stopFPDBStoreServer();

TEST_SUITE ("bitmap-pushdown" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-all-cached" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;
  std::string testQueryFileName = "test.sql";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {testQueryFileName,
                     testQueryFileName},
                    PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 1.0);

  stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("bitmap-pushdown-none-cached" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    p_partkey, p_size\n"
                             "from\n"
                             "    part\n"
                             "where\n"
                             "    p_size <= 2\n"
                             "order by\n"
                             "    p_size\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 0.0);

  stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at compute side, and both sides have some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-compute-bitmap-both-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_returnflag, l_quantity\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "order by\n"
                             "    l_returnflag\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at compute side, and only storage side has some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-compute-bitmap-only-storage-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_shipdate, l_discount\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "order by\n"
                             "    l_discount\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_TPCH_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FPDB_STORE_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

}

}

