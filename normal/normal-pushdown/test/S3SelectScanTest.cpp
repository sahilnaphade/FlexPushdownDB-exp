//
// Created by matt on 5/3/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/pushdown/cache/CacheLoad.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/s3/S3SelectScan2.h>
#include <normal/pushdown/merge/MergeOperator.h>
#include <normal/pushdown/Collate.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/core/Normal.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/connector/s3/S3SelectPartition.h>

#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::cache;
using namespace normal::pushdown::merge;
using namespace normal::pushdown::test;
using namespace normal::core;
using namespace normal::core::graph;
using namespace normal::connector::s3;

void run(const std::string &s3Bucket,
		 const std::string &s3Object,
		 FileType fileType,
		 const std::vector<std::string> &columnNames) {

  normal::pushdown::AWSClient client;
  client.init();

  auto n = Normal::start();

  auto g = n->createQuery();

  auto s3Objects = std::vector{s3Object};
  auto partitionMap = S3Util::listObjects(s3Bucket, s3Objects, AWSClient::defaultS3Client());
  SPDLOG_DEBUG("Discovered partitions");
  for (auto &partition : partitionMap) {
	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
  }

  std::shared_ptr<Partition> partition = std::make_shared<S3SelectPartition>(s3Bucket, s3Object);
  auto cacheLoad = CacheLoad::make("cache-load", columnNames, partition, 0, partitionMap[s3Object]);
  g->put(cacheLoad);
  auto merge = MergeOperator::make("merge");
  g->put(merge);
  auto s3selectScan = S3SelectScan2::make("s3select-scan",
										  s3Bucket,
										  s3Object,
										  "select {} from S3Object",
										  0,
										  partitionMap[s3Object],
										  fileType,
										  columnNames,
										  S3SelectCSVParseOptions(",", "\n"),
										  AWSClient::defaultS3Client(),
										  false);
  g->put(s3selectScan);

  auto collate = std::make_shared<Collate>("collate", g->getId());
  g->put(collate);

  cacheLoad->setHitOperator(merge);
  merge->consume(cacheLoad);

  cacheLoad->setMissOperator(s3selectScan);
  s3selectScan->consume(cacheLoad);

  s3selectScan->produce(merge);
  merge->consume(s3selectScan);

  merge->produce(collate);
  collate->consume(merge);

  TestUtil::writeExecutionPlan(*g);

  g->boot();
  g->start();
  g->join();

  auto tuples = TupleSet2::create(collate->tuples());

  SPDLOG_DEBUG("Output:\n{}", tuples->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  n->stop();

  client.shutdown();
}

#define SKIP_SUITE false

TEST_SUITE ("s3select-scan" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("s3select-scan-v1-csv" * doctest::skip(false || SKIP_SUITE)) {

  normal::pushdown::AWSClient client;
  client.init();

  auto n = Normal::start();

  auto g = n->createQuery();

  auto s3selectScan = S3SelectScan::make("s3selectscan",
										 "s3filter",
										 "ssb-sf0.01/supplier.tbl",
										 "select s_name from S3Object",
										 {"s_name"},
										 0,
										 std::numeric_limits<long>::max(),
										 S3SelectCSVParseOptions(",", "\n"),
										 AWSClient::defaultS3Client());
  g->put(s3selectScan);

  auto collate = std::make_shared<Collate>("collate", g->getId());
  g->put(collate);

  s3selectScan->produce(collate);
  collate->consume(s3selectScan);

  TestUtil::writeExecutionPlan(*g);

  g->boot();

  g->start();
  g->join();

  auto tuples = TupleSet2::create(collate->tuples());

  SPDLOG_DEBUG("Output:\n{}", tuples->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  n->stop();

  client.shutdown();
}

TEST_CASE ("s3select-scan-v2-csv" * doctest::skip(false || SKIP_SUITE)) {
  run("s3filter", "ssb-sf0.01/supplier.tbl", FileType::CSV, {"s_suppkey", "s_name"});
}

TEST_CASE ("s3select-scan-v2-csv-empty" * doctest::skip(false || SKIP_SUITE)) {
  run("s3filter", "ssb-sf0.01/supplier.tbl", FileType::CSV, {});
}

TEST_CASE ("s3select-scan-v2-parquet" * doctest::skip(false || SKIP_SUITE)) {
  run("s3filter", "ssb-sf0.01/parquet/supplier.snappy.parquet", FileType::Parquet, {"s_suppkey", "s_name"});
}

TEST_CASE ("s3select-scan-v2-parquet-empty" * doctest::skip(false || SKIP_SUITE)) {
  run("s3filter", "ssb-sf0.01/parquet/supplier.snappy.parquet", FileType::Parquet, {});
}

}
