//
// Created by matt on 6/7/20.
//

#include "normal/ssb/query1_1/Tests.h"

#include <doctest/doctest.h>
#include <filesystem>
#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/LocalFileSystemQueries.h>
#include <normal/ssb/query1_1/SQL.h>
#include <normal/ssb/TestUtil.h>

using namespace normal::ssb;
using namespace normal::ssb::query1_1;


/**
 * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1
 *
 * Only checking row count at moment
 */
void Tests::dateScan(const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::dateScan(dataDir,
																				numConcurrentUnits));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::dateScan("temp"),
								  {std::filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void Tests::dateFilter(short year, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, year, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::dateFilter(dataDir,
																				  year,
																				  numConcurrentUnits));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::dateFilter(year, "temp"),
								  {std::filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
 *
 * Only checking row count at moment
 */
void Tests::lineOrderScan(const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::lineOrderScan(dataDir,
																					 numConcurrentUnits));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::lineOrderScan("temp"),
								  {filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void Tests::lineOrderFilter(short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::lineOrderFilter(dataDir,
																					   discount, quantity,
																					   numConcurrentUnits));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::lineOrderFilter(discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for join component of query 1.1
 *
 * Only checking row count at moment
 */
void Tests::join(short year, short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::join(dataDir,
																			year, discount, quantity,
																			numConcurrentUnits));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::join(year, discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/date.tbl"),
								   filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for full query 1.1
 */
void Tests::full(short year, short discount, short quantity,
				 const std::string &dataDir,
				 int numConcurrentUnits,
				 bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan(LocalFileSystemQueries::full(dataDir,
																			year, discount, quantity,
																			numConcurrentUnits));

  auto actualName = actual->getColumnByIndex(0).value()->getName();
  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();

  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/date.tbl"),
								   filesystem::absolute(dataDir + "/lineorder.tbl")});
	auto expectedName = expected->at(0).at(0).first;
	auto expectedValue = std::stod(expected->at(0).at(0).second);
	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
	CHECK_EQ(expectedName, actualName);
	CHECK_EQ(expectedValue, actualValue);
  }
}