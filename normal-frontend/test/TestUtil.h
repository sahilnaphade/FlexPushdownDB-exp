//
// Created by Yifei Yang on 11/30/21.
//

#ifndef NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H
#define NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H

#include <normal/frontend/ActorSystemConfig.h>
#include <normal/executor/Executor.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/catalogue/CatalogueEntry.h>
#include <normal/calcite/CalciteClient.h>
#include <normal/aws/AWSClient.h>
#include <memory>
#include <vector>
#include <string>

using namespace normal::frontend;
using namespace normal::executor;
using namespace normal::plan;
using namespace normal::cache;
using namespace normal::catalogue;
using namespace normal::calcite;
using namespace normal::aws;
using namespace std;

namespace normal::frontend::test {

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
                                      bool isDistributed);

  TestUtil(const string &schemaName,
           const vector<string> &queryFileNames,
           int parallelDegree,
           bool isDistributed);

private:
  void runTest();
  void makeAWSClient();
  void makeCatalogueEntry();
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

  // internal parameters
  shared_ptr<AWSClient> awsClient_;
  shared_ptr<Catalogue> catalogue_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
  shared_ptr<CalciteClient> calciteClient_;
  shared_ptr<ActorSystemConfig> actorSystemConfig_;
  shared_ptr<::caf::actor_system> actorSystem_;
  vector<::caf::node_id> nodes_;
  shared_ptr<Executor> executor_;
  shared_ptr<Mode> mode_;
  shared_ptr<CachingPolicy> cachingPolicy_;

};

}

#endif //NORMAL_NORMAL_FRONTEND_TEST_TESTUTIL_H
