//
// Created by Yifei Yang on 12/1/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include <string>
#include <arrow/api.h>
#include <iostream>
#include <normal/expression/gandiva/NumericLiteral.h>

namespace normal::frontend::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-original-01" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/", {"tpch/original/01.sql"});
}

TEST_CASE ("temp" * doctest::skip(true)) {
  auto a = normal::expression::gandiva::num_lit<arrow::Date64Type>(19981201);
}

}

}
