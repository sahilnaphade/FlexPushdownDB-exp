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

TEST_CASE ("tpch-original-04" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/04.sql"});
}

TEST_CASE ("tpch-original-05" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/05.sql"});
}

TEST_CASE ("tpch-original-06" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/06.sql"});
}

TEST_CASE ("tpch-original-07" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/07.sql"});
}

TEST_CASE ("tpch-original-08" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/08.sql"});
}

TEST_CASE ("tpch-original-09" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/09.sql"});
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
  unique_ptr<arrow::ArrayBuilder> arrayBuilder;
  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &arrayBuilder);
//  arrayBuilder->AppendNulls(0);
  std::cout << status.ok() << std::endl;
}

}

}
