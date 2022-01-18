//
// Created by Yifei Yang on 11/30/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include <string>

namespace normal::frontend::test {

#define SKIP_SUITE true

TEST_SUITE ("ssb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-original-1.1" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/1.1.sql"
                                    }, 1);
}

TEST_CASE ("ssb-original-1.2" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/1.2.sql"},
                                    1);
}

TEST_CASE ("ssb-original-1.3" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/1.3.sql"},
                                    1);
}

TEST_CASE ("ssb-original-1.4" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/1.4.sql"},
                                    1);
}

TEST_CASE ("ssb-original-2.1" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/2.1.sql"},
                                    1);
}

TEST_CASE ("ssb-original-2.2" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/2.2.sql"},
                                    1);
}

TEST_CASE ("ssb-original-2.3" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/2.3.sql"},
                                    1);
}

TEST_CASE ("ssb-original-3.1" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/3.1.sql"},
                                    1);
}

TEST_CASE ("ssb-original-3.2" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/3.2.sql"},
                                    1);
}

TEST_CASE ("ssb-original-3.3" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/3.3.sql"},
                                    1);
}

TEST_CASE ("ssb-original-3.4" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/3.4.sql"},
                                    1);
}

TEST_CASE ("ssb-original-4.1" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/4.1.sql"},
                                    1);
}

TEST_CASE ("ssb-original-4.2" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/4.2.sql"},
                                    1);
}

TEST_CASE ("ssb-original-4.3" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/original/4.3.sql"},
                                    1);
}

TEST_CASE ("ssb-generated-1" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/generated/1.sql"},
                                    1);
}

TEST_CASE ("ssb-generated-2" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/generated/2.sql"},
                                    1);
}

TEST_CASE ("ssb-generated-3" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/generated/3.sql"},
                                    1);
}

TEST_CASE ("ssb-generated-4" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/generated/4.sql"},
                                    1);
}

TEST_CASE ("ssb-generated-5" * doctest::skip(true || SKIP_SUITE)) {
  TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                    {"ssb/generated/5.sql"},
                                    1);
}
}

}
