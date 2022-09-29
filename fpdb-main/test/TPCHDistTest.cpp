//
// Created by Yifei Yang on 2/4/22.
//

#include <fpdb/executor/physical/Globals.h>
#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (multiple compute nodes, S3)
 *
 * Start Calcite server on the coordinator (locally) and CAF server on all executors (remote nodes) before running this
 * (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf0.01-distributed-caf" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-distributed-caf-01" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-02" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-03" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-04" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-05" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-06" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-07" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-08" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-09" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-10" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-11" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-12" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-13" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-14" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-15" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-16" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-17" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-18" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-19" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-20" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-21" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-caf-22" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = false;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

}

/**
 * Start Calcite server on the coordinator and CAF server on all executors (remote nodes) before running this.
 * The coordinator cannot be any of the executor because each flight server needs a diff.
 * (see README for more details)
 */
TEST_SUITE ("tpch-sf0.01-distributed-flight" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf0.01-distributed-flight-01" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-02" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-03" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-04" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-05" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-06" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-07" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-08" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-09" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-10" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-11" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-12" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-13" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-14" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-15" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-16" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-17" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-18" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-19" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-20" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-21" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf0.01-distributed-flight-22" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::USE_FLIGHT_COMM = true;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_TPCH_DIST_SF0_01,
                                            true,
                                            ObjStoreType::S3));
}

}

}
