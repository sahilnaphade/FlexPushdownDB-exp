//
// Created by Yifei Yang on 11/30/21.
//

#include "TestUtil.h"
#include <normal/frontend/CAFInit.h>
#include <normal/frontend/ExecConfig.h>
#include <normal/executor/physical/transform/PrePToPTransformer.h>
#include <normal/calcite/CalciteConfig.h>
#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/aws/AWSConfig.h>
#include <normal/util/Util.h>

using namespace normal::executor::physical;
using namespace normal::plan::calcite;
using namespace normal::catalogue::s3;
using namespace normal::util;

namespace normal::frontend::test {

TestUtil::TestUtil(const string &schemaName,
                   const vector<string> &queryFileNames,
                   int parallelDegree,
                   bool isDistributed) :
  schemaName_(schemaName),
  queryFileNames_(queryFileNames),
  parallelDegree_(parallelDegree),
  isDistributed_(isDistributed) {}

bool TestUtil::e2eNoStartCalciteServer(const string &schemaName,
                                       const vector<string> &queryFileNames,
                                       int parallelDegree,
                                       bool isDistributed) {
  TestUtil testUtil(schemaName, queryFileNames, parallelDegree, isDistributed);
  try {
    testUtil.runTest();
    return true;
  } catch (const runtime_error &err) {
    cout << err.what() << endl;
    return false;
  }
}

void TestUtil::runTest() {
  spdlog::set_level(spdlog::level::info);

  // AWS client
  makeAWSClient();

  // Catalogue entry
  makeCatalogueEntry();

  // Calcite client
  makeCalciteClient();

  // mode and caching policy
  mode_ = Mode::pullupMode();
  cachingPolicy_ = nullptr;

  // create the executor
  makeExecutor();

  for (const auto &queryFileName: queryFileNames_) {
    executeQueryFile(queryFileName);
  }

  // stop
  stop();
}

void TestUtil::makeAWSClient() {
  awsClient_ = make_shared<AWSClient>(
          make_shared<AWSConfig>(normal::aws::S3, 0));
  awsClient_->init();
}

void TestUtil::makeCatalogueEntry() {
  // create the catalogue
  string s3Bucket = "flexpushdowndb";
  filesystem::path metadataPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/metadata");
  catalogue_ = make_shared<Catalogue>("main", metadataPath);

  // read catalogue entry
  catalogueEntry_ = S3CatalogueEntryReader::readS3CatalogueEntry(catalogue_,
                                                                 s3Bucket,
                                                                 schemaName_,
                                                                 awsClient_->getS3Client());
  catalogue_->putEntry(catalogueEntry_);
}

void TestUtil::makeCalciteClient() {
  auto calciteConfig = CalciteConfig::parseCalciteConfig();
  calciteClient_ = make_shared<CalciteClient>(calciteConfig);
  calciteClient_->startClient();
}

void TestUtil::connect() {
  if (!actorSystemConfig_->nodeIps_.empty()) {
    for (const auto &nodeIp: actorSystemConfig_->nodeIps_) {
      auto expectedNode = actorSystem_->middleman().connect(nodeIp, actorSystemConfig_->port_);
      if (!expectedNode) {
        nodes_.clear();
        throw runtime_error(
                fmt::format("Failed to connected to server {}: {}", nodeIp, to_string(expectedNode.error())));
      }
      nodes_.emplace_back(*expectedNode);
    }
  }
}

void TestUtil::makeExecutor() {
  // create the actor system
  const auto &remoteIps = readRemoteIps();
  int CAFServerPort = ExecConfig::parseCAFServerPort();
  actorSystemConfig_ = make_shared<ActorSystemConfig>(CAFServerPort, remoteIps, false);
  CAFInit::initCAFGlobalMetaObjects();
  actorSystem_ = make_shared<::caf::actor_system>(*actorSystemConfig_);

  // create the executor
  if (isDistributed_) {
    connect();
  }
  executor_ = make_shared<Executor>(actorSystem_,
                                    nodes_,
                                    mode_,
                                    cachingPolicy_,
                                    true,
                                    false);
  executor_->start();
}

void TestUtil::executeQueryFile(const string &queryFileName) {
  cout << "Query: " << queryFileName << endl;

  // Plan query
  string queryPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/query")
          .append(queryFileName)
          .string();
  string query = readFile(queryPath);
  string planResult = calciteClient_->planQuery(query, schemaName_);

  // deserialize plan json string into prephysical plan
  auto planDeserializer = make_shared<CalcitePlanJsonDeserializer>(planResult, catalogueEntry_);
  const auto &prePhysicalPlan = planDeserializer->deserialize();

  // trim unused fields (Calcite trimmer does not trim completely)
  prePhysicalPlan->populateAndTrimProjectColumns();

  // transform prephysical plan to physical plan
  auto prePToPTransformer = make_shared<PrePToPTransformer>(prePhysicalPlan,
                                                            awsClient_,
                                                            mode_,
                                                            parallelDegree_,
                                                            nodes_.size() + 1);
  const auto &physicalPlan = prePToPTransformer->transform();

  // execute
  const auto &execRes = executor_->execute(physicalPlan);

  // show output
  stringstream ss;
  ss << fmt::format("Result |\n{}", execRes.first->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 10000)));
  ss << fmt::format("\nTime: {} secs", (double) (execRes.second) / 1000000000.0);
  ss << endl;
  cout << ss.str() << endl;
}

void TestUtil::stop() {
  // need to shutdown awsClient first
  awsClient_->shutdown();
  executor_->stop();
}

}
