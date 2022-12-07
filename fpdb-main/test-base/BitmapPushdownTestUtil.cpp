//
// Created by Yifei Yang on 12/6/22.
//

#include "BitmapPushdownTestUtil.h"
#include "TestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/plan/prephysical/separable/Globals.h>
#include <doctest/doctest.h>
#include <fmt/format.h>

namespace fpdb::main::test {

void BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(const std::string &cachingQuery,
                                                                 const std::string &testQuery,
                                                                 bool isSsb,
                                                                 const std::string &sf,
                                                                 int parallelDegree,
                                                                 bool startFPDBStore) {
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  if (startFPDBStore) {
    startFPDBStoreServer();
  }
  TestUtil testUtil(isSsb ? fmt::format("ssb-sf{}/parquet/", sf) : fmt::format("tpch-sf{}/parquet/", sf),
                    {cachingQueryFileName,
                     testQueryFileName},
                    parallelDegree,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    std::numeric_limits<int64_t>::max());

  // enable filter bitmap pushdown
  std::unordered_map<std::string, bool> flags;
  set_pushdown_flags(flags);

  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  if (startFPDBStore) {
    stopFPDBStoreServer();
  }

  // reset pushdown flags
  reset_pushdown_flags(flags);

  // clear query file
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

void BitmapPushdownTestUtil::set_pushdown_flags(std::unordered_map<std::string, bool> &flags) {
  flags["group"] = fpdb::executor::physical::ENABLE_GROUP_BY_PUSHDOWN;
  flags["bloom_filter"] = fpdb::executor::physical::ENABLE_BLOOM_FILTER_PUSHDOWN;
  flags["shuffle"] = fpdb::executor::physical::ENABLE_SHUFFLE_PUSHDOWN;
  flags["co_located_join"] = fpdb::plan::prephysical::separable::ENABLE_CO_LOCATED_JOIN_PUSHDOWN;
  flags["filter_bitmap"] = fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN;
  fpdb::executor::physical::ENABLE_GROUP_BY_PUSHDOWN = false;
  fpdb::executor::physical::ENABLE_BLOOM_FILTER_PUSHDOWN = false;
  fpdb::executor::physical::ENABLE_SHUFFLE_PUSHDOWN = false;
  fpdb::plan::prephysical::separable::ENABLE_CO_LOCATED_JOIN_PUSHDOWN = false;
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
}

void BitmapPushdownTestUtil::reset_pushdown_flags(std::unordered_map<std::string, bool> &flags) {
  fpdb::executor::physical::ENABLE_GROUP_BY_PUSHDOWN = flags["group"];
  fpdb::executor::physical::ENABLE_BLOOM_FILTER_PUSHDOWN = flags["bloom_filter"];
  fpdb::executor::physical::ENABLE_SHUFFLE_PUSHDOWN = flags["shuffle"];
  fpdb::plan::prephysical::separable::ENABLE_CO_LOCATED_JOIN_PUSHDOWN = flags["co_located_join"];
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = flags["filter_bitmap"];
}

std::shared_ptr<fpdb::store::server::Server> BitmapPushdownTestUtil::fpdbStoreServer_ = nullptr;
std::shared_ptr<fpdb::store::server::caf::ActorManager> BitmapPushdownTestUtil::actorManager_= nullptr;
std::shared_ptr<fpdb::store::client::FPDBStoreClientConfig> BitmapPushdownTestUtil::fpdbStoreClientConfig_= nullptr;

void BitmapPushdownTestUtil::startFPDBStoreServer() {
  fpdbStoreClientConfig_ = fpdb::store::client::FPDBStoreClientConfig::parseFPDBStoreClientConfig();
  actorManager_ = fpdb::store::server::caf::ActorManager::make<::caf::id_block::Server>().value();
  fpdbStoreServer_ = fpdb::store::server::Server::make(
          fpdb::store::server::ServerConfig{"1",
                                            0,
                                            true,
                                            std::nullopt,
                                            0,
                                            fpdbStoreClientConfig_->getFlightPort(),
                                            fpdbStoreClientConfig_->getFileServicePort(),
                                            "test-resources/fpdb-store",
                                            1},
          std::nullopt,
          actorManager_);
  auto initResult = fpdbStoreServer_->init();
          REQUIRE(initResult.has_value());
  auto startResult = fpdbStoreServer_->start();
          REQUIRE(startResult.has_value());
}

void BitmapPushdownTestUtil::stopFPDBStoreServer() {
  fpdbStoreServer_->stop();
  fpdbStoreServer_.reset();
  actorManager_.reset();
}

}
