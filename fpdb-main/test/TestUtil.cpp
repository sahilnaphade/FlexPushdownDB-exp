//
// Created by Yifei Yang on 11/30/21.
//

#include "TestUtil.h"
#include <fpdb/main/ExecConfig.h>
#include <fpdb/executor/caf/CAFInit.h>
#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/cache/policy/LRUCachingPolicy.h>
#include <fpdb/cache/policy/LFUCachingPolicy.h>
#include <fpdb/cache/policy/LFUSCachingPolicy.h>
#include <fpdb/cache/policy/WLFUCachingPolicy.h>
#include <fpdb/calcite/CalciteConfig.h>
#include <fpdb/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <fpdb/plan/prephysical/separable/SeparablePrePOpTransformer.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntryReader.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fpdb/aws/AWSConfig.h>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <fpdb/util/Util.h>

using namespace fpdb::executor::physical;
using namespace fpdb::cache;
using namespace fpdb::plan::calcite;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::catalogue::obj_store;
using namespace fpdb::store::client;
using namespace fpdb::util;

namespace fpdb::main::test {

TestUtil::TestUtil(const string &schemaName,
                   const vector<string> &queryFileNames,
                   int parallelDegree,
                   bool isDistributed,
                   ObjStoreType objStoreType,
                   const shared_ptr<Mode> &mode,
                   CachingPolicyType cachingPolicyType,
                   size_t cacheSize) :
  schemaName_(schemaName),
  queryFileNames_(queryFileNames),
  parallelDegree_(parallelDegree),
  isDistributed_(isDistributed),
  objStoreType_(objStoreType),
  mode_(mode),
  cachingPolicyType_(cachingPolicyType),
  cacheSize_(cacheSize) {}

bool TestUtil::e2eNoStartCalciteServer(const string &schemaName,
                                       const vector<string> &queryFileNames,
                                       int parallelDegree,
                                       bool isDistributed,
                                       ObjStoreType objStoreType,
                                       const shared_ptr<Mode> &mode,
                                       CachingPolicyType cachingPolicyType,
                                       size_t cacheSize) {
  TestUtil testUtil(schemaName,
                    queryFileNames,
                    parallelDegree,
                    isDistributed,
                    objStoreType,
                    mode,
                    cachingPolicyType,
                    cacheSize);
  try {
    testUtil.runTest();
    return true;
  } catch (const runtime_error &err) {
    cout << err.what() << endl;
    return false;
  }
}

void TestUtil::writeQueryToFile(const std::string queryFileName, const std::string query) {
  std::string queryFilePath = std::filesystem::current_path()
          .parent_path()
          .append("resources/query")
          .append(queryFileName)
          .string();
  writeFile(queryFilePath, query);
}

void TestUtil::removeQueryFile(const std::string queryFileName) {
  std::string queryFilePath = std::filesystem::current_path()
          .parent_path()
          .append("resources/query")
          .append(queryFileName)
          .string();
  std::remove(queryFilePath.c_str());
}

double TestUtil::getCrtQueryHitRatio() const {
  return crtQueryHitRatio_;
}

void TestUtil::runTest() {
  spdlog::set_level(spdlog::level::info);

  // Object store connector
  makeObjStoreConnector();

  // Catalogue entry
  makeCatalogueEntry();

  // Caching policy
  makeCachingPolicy();

  // Calcite client
  makeCalciteClient();

  // create the executor
  makeExecutor();

  for (const auto &queryFileName: queryFileNames_) {
    executeQueryFile(queryFileName);
  }

  // stop
  stop();
}

void TestUtil::makeObjStoreConnector() {
  switch (objStoreType_) {
    case ObjStoreType::S3: {
      auto awsClient = make_shared<AWSClient>(make_shared<AWSConfig>(fpdb::aws::S3, 0));
      awsClient->init();
      objStoreConnector_ = make_shared<S3Connector>(awsClient);
      return;
    }
    case ObjStoreType::FPDB_STORE: {
      auto fpdbStoreClientConfig = FPDBStoreClientConfig::parseFPDBStoreClientConfig();
      objStoreConnector_ = make_shared<FPDBStoreConnector>(fpdbStoreClientConfig->getHost(),
                                                           fpdbStoreClientConfig->getFileServicePort(),
                                                           fpdbStoreClientConfig->getFlightPort());
      return;
    }
    default:
      throw runtime_error("Unknown object store type");
  }
}

void TestUtil::makeCatalogueEntry() {
  // create the catalogue
  string s3Bucket = "flexpushdowndb";
  filesystem::path metadataPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/metadata");
  catalogue_ = make_shared<Catalogue>("main", metadataPath);

  // read catalogue entry
  catalogueEntry_ = ObjStoreCatalogueEntryReader::readCatalogueEntry(catalogue_,
                                                                     s3Bucket,
                                                                     schemaName_,
                                                                     objStoreConnector_);
  catalogue_->putEntry(catalogueEntry_);
}

void TestUtil::makeCachingPolicy() {
  switch (cachingPolicyType_) {
    case CachingPolicyType::LFU: {
      cachingPolicy_ = make_shared<LFUCachingPolicy>(cacheSize_, catalogueEntry_);
      return;
    }
    case CachingPolicyType::LFUS: {
      cachingPolicy_ = make_shared<LFUSCachingPolicy>(cacheSize_, catalogueEntry_);
      return;
    }
    case CachingPolicyType::LRU: {
      cachingPolicy_ = make_shared<LRUCachingPolicy>(cacheSize_, catalogueEntry_);
      return;
    }
    case CachingPolicyType::WLFU: {
      cachingPolicy_ = make_shared<WLFUCachingPolicy>(cacheSize_, catalogueEntry_);
      return;
    }
    case CachingPolicyType::NONE: {
      cachingPolicy_ = nullptr;
      return;
    }
    default:
      throw runtime_error(fmt::format("Unknown caching policy: {}", cachingPolicyType_));
  }
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
  fpdb::executor::caf::CAFInit::initCAFGlobalMetaObjects();
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

  // detect separable operators
  auto separableTransformer = make_shared<SeparablePrePOpTransformer>(catalogueEntry_);
  separableTransformer->transform(prePhysicalPlan);

  // transform prephysical plan to physical plan
  int numNodes = isDistributed_ ? (int) nodes_.size() : 1;
  auto prePToPTransformer = make_shared<PrePToPTransformer>(prePhysicalPlan,
                                                            catalogueEntry_,
                                                            objStoreConnector_,
                                                            mode_,
                                                            parallelDegree_,
                                                            numNodes);
  const auto &physicalPlan = prePToPTransformer->transform();

  // execute
  const auto &execRes = executor_->execute(physicalPlan, isDistributed_);

  // show output
  stringstream ss;
  ss << fmt::format("\nResult |\n{}", execRes.first->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 100)));
  ss << fmt::format("\nTime: {} secs\n\n", (double) (execRes.second) / 1000000000.0);
  ss << endl;
  cout << ss.str() << endl;

  // metrics used for checking in some unit tests
  crtQueryHitRatio_ = executor_->getCrtQueryHitRatio();
}

void TestUtil::stop() {
  // need to shutdown awsClient first
  if (objStoreType_ == ObjStoreType::S3) {
    auto s3Connector = static_pointer_cast<S3Connector>(objStoreConnector_);
    s3Connector->getAwsClient()->shutdown();
  }
  executor_->stop();
}

}
