//
// Created by Yifei Yang on 11/30/21.
//

#include "TestUtil.h"
#include <normal/executor/Executor.h>
#include <normal/executor/physical/transform/PrePToPTransformer.h>
#include <normal/calcite/CalciteConfig.h>
#include <normal/calcite/CalciteClient.h>
#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/plan/Mode.h>
#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/aws/AWSClient.h>
#include <normal/aws/AWSConfig.h>
#include <normal/util/Util.h>

using namespace normal::executor;
using namespace normal::executor::physical;
using namespace normal::cache;
using namespace normal::calcite;
using namespace normal::plan::calcite;
using namespace normal::plan;
using namespace normal::catalogue::s3;
using namespace normal::util;
using namespace normal::aws;
using namespace Aws::S3;

namespace normal::frontend::test {

void TestUtil::e2eNoStartCalciteServer(const string &schemaName,
                                       const vector<string> &queryFileNames) {
//  spdlog::set_level(spdlog::level::debug);

  // AWS client
  shared_ptr<AWSClient> awsClient = make_shared<AWSClient>(
          make_shared<AWSConfig>(normal::aws::S3, 0));
  awsClient->init();

  // make catalogue
  string s3Bucket = "flexpushdowndb";
  filesystem::path metadataPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/metadata");
  shared_ptr<Catalogue> catalogue = make_shared<Catalogue>("main", metadataPath);

  // read catalogue entry
  const auto &s3CatalogueEntry = S3CatalogueEntryReader::readS3CatalogueEntry(catalogue,
                                                                              s3Bucket,
                                                                              schemaName,
                                                                              awsClient->getS3Client());
  catalogue->putEntry(s3CatalogueEntry);

  // Create and start Calcite client
  shared_ptr<CalciteConfig> calciteConfig = CalciteConfig::parseCalciteConfig();
  CalciteClient calciteClient(calciteConfig);
  calciteClient.startClient();

  // mode, caching policy, executor
  const auto &mode = Mode::pullupMode();
  const auto &cachingPolicy = nullptr;

  const auto &executor = make_shared<Executor>(mode, cachingPolicy, true, false);
  executor->start();

  for (const auto &queryFileName: queryFileNames) {
    cout << "Query: " << queryFileName << endl;

    // Plan query
    string queryPath = std::filesystem::current_path()
            .parent_path()
            .append("resources/query")
            .append(queryFileName)
            .string();
    string query = readFile(queryPath);
    string planResult = calciteClient.planQuery(query, schemaName);

    // deserialize plan json string into prephysical plan
    auto planDeserializer = make_shared<CalcitePlanJsonDeserializer>(planResult, s3CatalogueEntry);
    const auto &prePhysicalPlan = planDeserializer->deserialize();

    // trim unused fields (Calcite trimmer does not trim completely)
    prePhysicalPlan->populateAndTrimProjectColumns();

    // transform prephysical plan to physical plan
    auto prePToPTransformer = make_shared<PrePToPTransformer>(prePhysicalPlan, awsClient, mode, 1);
    const auto &physicalPlan = prePToPTransformer->transform();

    // execute
    const auto &execRes = executor->execute(physicalPlan);

    // show output
    stringstream ss;
    ss << fmt::format("Result |\n{}", execRes.first->showString(
            TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
    ss << fmt::format("\nTime: {} secs", (double) (execRes.second) / 1000000000.0);
    ss << endl;
    cout << ss.str() << endl;
  }

  // stop, need to shutdown awsClient first
  awsClient->shutdown();
  executor->stop();
}

}
