//
// Created by Yifei Yang on 11/30/21.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_TESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_TESTUTIL_H

#include <fpdb/main/ActorSystemConfig.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/catalogue/Catalogue.h>
#include <fpdb/catalogue/CatalogueEntry.h>
#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>
#include <fpdb/catalogue/obj-store/ObjStoreType.h>
#include <fpdb/calcite/CalciteClient.h>
#include <fpdb/aws/AWSClient.h>
#include <memory>
#include <vector>
#include <string>

using namespace fpdb::main;
using namespace fpdb::executor;
using namespace fpdb::plan;
using namespace fpdb::cache;
using namespace fpdb::catalogue;
using namespace fpdb::catalogue::obj_store;
using namespace fpdb::calcite;
using namespace fpdb::aws;
using namespace std;

namespace fpdb::main::test {

class TestUtil {

public:
  /**
   * Test with calcite server already started, using pullup by default
   * @param schemaName
   * @param queryFileNames
   * @param parallelDegree
   * @param isDistributed
   *
   * @return whether executed successfully
   */
  static bool e2eNoStartCalciteServer(const string &schemaName,
                                      const vector<string> &queryFileNames,
                                      int parallelDegree,
                                      bool isDistributed,
                                      ObjStoreType objStoreType,
                                      const shared_ptr<Mode> &mode = Mode::pullupMode(),
                                      CachingPolicyType cachingPolicyType = CachingPolicyType::NONE,
                                      size_t cacheSize = 1L * 1024 * 1024 * 1024);

  static void writeQueryToFile(const std::string queryFileName, const std::string query);
  static void removeQueryFile(const std::string queryFileName);

  TestUtil(const string &schemaName,
           const vector<string> &queryFileNames,
           int parallelDegree,
           bool isDistributed,
           ObjStoreType objStoreType,
           const shared_ptr<Mode> &mode,
           CachingPolicyType cachingPolicyType,
           size_t cacheSize);

  double getCrtQueryHitRatio() const;
  void setFixLayoutIndices(const set<int> &fixLayoutIndices);

  void runTest();

private:
  void makeObjStoreConnector();
  void makeCatalogueEntry();
  void makeCachingPolicy();
  void makeCalciteClient();
  void connect();
  void makeExecutor();
  void executeQueryFile(const string &queryFileName);
  void stop();

  // input parameters
  std::string schemaName_;
  vector<string> queryFileNames_;
  int parallelDegree_;
  bool isDistributed_;
  ObjStoreType objStoreType_;
  shared_ptr<Mode> mode_;
  CachingPolicyType cachingPolicyType_;
  size_t cacheSize_;

  // internal parameters
  shared_ptr<CachingPolicy> cachingPolicy_;
  shared_ptr<ObjStoreConnector> objStoreConnector_;
  shared_ptr<Catalogue> catalogue_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
  shared_ptr<CalciteClient> calciteClient_;
  shared_ptr<ActorSystemConfig> actorSystemConfig_;
  shared_ptr<::caf::actor_system> actorSystem_;
  vector<::caf::node_id> nodes_;
  shared_ptr<Executor> executor_;

  // used to fix cache layout for testing
  set<int> fixLayoutIndices_;

  // metrics used for checking in some unit tests
  double crtQueryHitRatio_;

};

}

#endif //FPDB_FPDB_MAIN_TEST_BASE_TESTUTIL_H