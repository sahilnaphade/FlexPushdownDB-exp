//
// Created by Yifei Yang on 12/1/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"

namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf0.01-single_node-no-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            1,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-no-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            1,
                                            false));
}

}

TEST_SUITE ("tpch-sf0.01-single_node-parallel" * doctest::skip(SKIP_SUITE)) {
  
int parallelDegree = 3;

TEST_CASE ("tpch-sf0.01-single_node-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegree,
                                            false));
}

TEST_CASE ("tpch-sf0.01-single_node-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegree,
                                            false));
}

}

TEST_SUITE ("tpch-sf0.01-distributed" * doctest::skip(SKIP_SUITE)) {
  
int parallelDegree = 2;

TEST_CASE ("tpch-sf0.01-distributed-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            parallelDegree,
                                            true));
}

TEST_CASE ("tpch-sf0.01-distributed-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            parallelDegree,
                                            true));
}

}

TEST_SUITE ("tpch-sf10-distributed" * doctest::skip(SKIP_SUITE)) {

int parallelDegree = 8;

TEST_CASE ("tpch-sf10-distributed-01" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/01.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-02" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/02.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-03" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/03.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-04" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/04.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-05" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/05.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-06" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/06.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-07" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/07.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-08" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/08.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-09" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/09.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-10" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/10.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-11" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/11.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-12" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/12.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-13" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/13.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-14" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/14.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-15" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/15.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-16" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/16.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-17" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/17.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-18" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/18.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-19" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/19.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-20" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/20.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-21" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/21.sql"},
                                                    parallelDegree,
                                                    true));
}

TEST_CASE ("tpch-sf10-distributed-22" * doctest::skip(false || SKIP_SUITE)) {
          REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                                    {"tpch/original/22.sql"},
                                                    parallelDegree,
                                                    true));
}

}

}
