//
// Created by Yifei Yang on 12/1/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include <string>
#include <arrow/api.h>
#include <iostream>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/tuple/TupleSet.h>
#include <arrow/util/value_parsing.h>

using namespace normal::tuple;

namespace normal::frontend::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-original-01" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/01.sql"});
}

TEST_CASE ("tpch-original-02" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/02.sql"});
}

TEST_CASE ("tpch-original-03" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/03.sql"});
}

TEST_CASE ("tpch-original-05" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/05.sql"});
}

TEST_CASE ("tpch-original-06" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/06.sql"});
}

TEST_CASE ("tpch-original-10" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/10.sql"});
}

TEST_CASE ("tpch-original-12" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/12.sql"});
}

TEST_CASE ("tpch-original-14" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/14.sql"});
}

TEST_CASE ("tpch-original-19" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/19.sql"});
}

TEST_CASE ("temp" * doctest::skip(true)) {
  string dateStr = "1998-12-01";
  auto parser = arrow::TimestampParser::MakeISO8601();
  int64_t val;
  (*parser)(dateStr.c_str(), 10, arrow::TimeUnit::MILLI, &val);
  auto scalar = arrow::MakeScalar(arrow::date64(), val).ValueOrDie();
  std::cout << scalar->ToString() << std::endl;
}

}

}
