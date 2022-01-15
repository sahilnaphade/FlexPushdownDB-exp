//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/frontend/Client.h>
#include <normal/executor/physical/transform/PrePToPTransformer.h>
#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/util/Util.h>

using namespace normal::plan::calcite;
using namespace normal::catalogue::s3;
using namespace normal::util;

namespace normal::frontend {

path Client::getDefaultMetadataPath() {
  return current_path().parent_path().append("resources/metadata");
}

string Client::getDefaultCatalogueName() {
  return "main";
}

string Client::start() {
  // catalogue
  catalogue_ = make_shared<Catalogue>(getDefaultCatalogueName(), getDefaultMetadataPath());

  // AWS client
  const auto &awsConfig = AWSConfig::parseExecConfig();
  awsClient_ = make_shared<AWSClient>(awsConfig);
  awsClient_->init();
  SPDLOG_INFO("AWS client started");

  // calcite client
  const auto &calciteConfig = CalciteConfig::parseCalciteConfig();
  calciteClient_ = make_shared<CalciteClient>(calciteConfig);
  calciteClient_->startServer();
  SPDLOG_INFO("Calcite server started");
  calciteClient_->startClient();
  SPDLOG_INFO("Calcite client started");

  // execution config
  execConfig_ = ExecConfig::parseExecConfig(catalogue_, awsClient_);

  // actor system config
  const auto &remoteIps = readRemoteIps();
  actorSystemConfig_ = make_shared<ActorSystemConfig>(execConfig_->getCAFServerPort(),
                                                      remoteIps,
                                                      false);

  // executor
  executor_ = make_shared<Executor>(execConfig_->getMode(),
                                    execConfig_->getCachingPolicy(),
                                    execConfig_->showOpTimes(),
                                    execConfig_->showScanMetrics());
  executor_->start();
  SPDLOG_INFO("Executor started");

  return "Client started";
}

string Client::stop() {
  // AWS client
  awsClient_->shutdown();
  SPDLOG_INFO("AWS client stopped");

  // calcite client
  calciteClient_->shutdownServer();
  SPDLOG_INFO("Calcite server stopped");

  // executor
  executor_->stop();
  SPDLOG_INFO("Executor stopped");

  return "Client stopped";
}

string Client::restart() {
  stop();
  start();
  return "Client restarted";
}

string Client::executeQuery(const string &query) {
  // fetch catalogue entry
  const auto &catalogueEntry = getCatalogueEntry(execConfig_->getSchemaName());

  // plan
  const auto &physicalPlan = plan(query, catalogueEntry);

  // execute
  const auto execRes = execute(physicalPlan);

  // output
  stringstream ss;
  ss << fmt::format("Result |\n{}", execRes.first->showString(
          TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  ss << fmt::format("\nTime: {} secs", (double) (execRes.second) / 1000000000.0);
  ss << endl;
  return ss.str();
}

string Client::executeQueryFile(const string &queryFilePath) {
  const auto &query = readFile(queryFilePath);
  return executeQuery(query);
}

shared_ptr<CatalogueEntry> Client::getCatalogueEntry(const string &schemaName) {
  shared_ptr<CatalogueEntry> catalogueEntry;
  const auto expCatalogueEntry = catalogue_->getEntry(
          fmt::format("s3://{}/{}", execConfig_->getS3Bucket(), schemaName));
  if (expCatalogueEntry.has_value()) {
    return expCatalogueEntry.value();
  } else {
    catalogueEntry = S3CatalogueEntryReader::readS3CatalogueEntry(catalogue_,
                                                                  execConfig_->getS3Bucket(),
                                                                  schemaName,
                                                                  awsClient_->getS3Client());
    catalogue_->putEntry(catalogueEntry);
    return catalogueEntry;
  }
}

shared_ptr<PhysicalPlan> Client::plan(const string &query, const shared_ptr<CatalogueEntry> &catalogueEntry) {
  // calcite planning
  string planResult = calciteClient_->planQuery(query, execConfig_->getSchemaName());

  // deserialize plan json string into prephysical plan
  auto planDeserializer = make_shared<CalcitePlanJsonDeserializer>(planResult, catalogueEntry);
  const auto &prePhysicalPlan = planDeserializer->deserialize();

  // trim unused fields (Calcite trimmer does not trim completely)
  prePhysicalPlan->populateAndTrimProjectColumns();

  // transform prephysical plan to physical plan
  auto prePToPTransformer = make_shared<PrePToPTransformer>(prePhysicalPlan,
                                                            awsClient_,
                                                            execConfig_->getMode(),
                                                            execConfig_->getParallelDegree());
  const auto &physicalPlan = prePToPTransformer->transform();

  return physicalPlan;
}

pair<shared_ptr<TupleSet>, long> Client::execute(const shared_ptr<PhysicalPlan> &physicalPlan) {
  return executor_->execute(physicalPlan);;
}

}
