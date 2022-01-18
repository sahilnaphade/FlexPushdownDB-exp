//
// Created by Yifei Yang on 12/1/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include <string>

namespace normal::frontend::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf0.01-no-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-no-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/01.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/02.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/03.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/04.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/05.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/06.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/07.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/08.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/09.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/10.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/11.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/12.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/13.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/14.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/15.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/16.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/17.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/18.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/19.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/20.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/21.sql"},
                                    1);
}

TEST_CASE ("tpch-sf0.01-no-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/22.sql"},
                                    1);
}

}

TEST_SUITE ("tpch-sf0.01-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/01.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/02.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/03.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/04.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/05.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/06.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/07.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/08.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/09.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/10.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/11.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/12.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/13.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/14.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/15.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/16.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/17.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/18.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/19.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/20.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/21.sql"},
                                    3);
}

TEST_CASE ("tpch-sf0.01-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                    {"tpch/original/22.sql"},
                                    3);
}

}

}
