//
// Created by Yifei Yang on 3/28/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

using namespace fpdb::util;

/**
 * Test of hybrid execution, using TPC-H dataset
 * Using the same setup as TPCHFPDBStoreSameNodeTest
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

void startFPDBStoreServer();
void stopFPDBStoreServer();

TEST_SUITE ("hybrid" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("hybrid-all-cached" * doctest::skip(false || SKIP_SUITE)) {
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
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-none-cached" * doctest::skip(false || SKIP_SUITE)) {
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
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-partial-cached-filter-separable" * doctest::skip(false || SKIP_SUITE)) {
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

  // TODO: check

  stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-partial-cached-project-separable" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_extendedprice * l_discount as mul1\n"
                             "from\n"
                             "    lineitem\n"
                             "order by\n"
                             "    mul1\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_extendedprice * l_discount as mul1, l_extendedprice * l_quantity as mul2\n"
                          "from\n"
                          "    lineitem\n"
                          "order by\n"
                          "    mul1, mul2\n"
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

  // TODO: check

  stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-partial-cached-aggregate-separable" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    sum(l_extendedprice * l_discount) as sum1\n"
                             "from\n"
                             "    lineitem\n";
  std::string testQuery = "select\n"
                          "    sum(l_extendedprice * l_discount) as sum1, sum(l_extendedprice * l_quantity) as sum2\n"
                          "from\n"
                          "    lineitem\n";

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

  // TODO: check

  stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-partial-cached-filter-project-separable" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_extendedprice * l_discount as mul1\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "order by\n"
                             "    mul1\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_extendedprice * l_discount as mul1, l_extendedprice * l_quantity as mul2\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    mul1, mul2\n"
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

  // TODO: check

  stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("hybrid-partial-cached-filter-aggregate-separable" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    sum(l_extendedprice * l_discount) as sum1\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "from\n"
                             "    lineitem\n";
  std::string testQuery = "select\n"
                          "    sum(l_extendedprice * l_discount) as sum1, sum(l_extendedprice * l_quantity) as sum2\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "from\n"
                          "    lineitem\n";

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

  // TODO: check

  stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

}

}
