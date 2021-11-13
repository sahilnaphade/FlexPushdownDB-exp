//
// Created by Yifei Yang on 10/30/21.
//

#include <normal/calcite/CalciteConfig.h>
#include <normal/calcite/CalciteClient.h>
#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/catalogue/s3/S3CatalogueEntryReader.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/aws/AWSClient.h>
#include <normal/aws/AWSConfig.h>
#include <normal/util/Util.h>

#include <memory>
#include <iostream>
#include <filesystem>

using namespace normal::calcite;
using namespace normal::plan::calcite;
using namespace normal::catalogue::s3;
using namespace normal::util;
using namespace normal::aws;
using namespace Aws::S3;
using namespace std;

void e2eWithServer() {
  // Create Calcite client
  shared_ptr<CalciteConfig> calciteConfig = parseCalciteConfig();
  CalciteClient calciteClient(calciteConfig);

  // Start Calcite server and client
  calciteClient.startServer();
  calciteClient.startClient();

  // Plan query
  string queryPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/query/ssb/3.1.sql")
          .string();
  string query = readFile(queryPath);
  string schemaName = "ssb-sf1-sortlineorder/csv";
  string planResult = calciteClient.planQuery(query, schemaName);
  cout << planResult << endl;

  // Shutdown Calcite server
  calciteClient.shutdownServer();
}

void e2eWithoutServer() {
  // Create and start Calcite client
  shared_ptr<CalciteConfig> calciteConfig = parseCalciteConfig();
  CalciteClient calciteClient(calciteConfig);
  calciteClient.startClient();

  // Plan query
  string queryPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/query/ssb/4.1.sql")
          .string();
  string query = readFile(queryPath);
  string schemaName = "ssb-sf1-sortlineorder/csv/";
  string planResult = calciteClient.planQuery(query, schemaName);
  cout << planResult << endl;

  // make catalogue
  string s3Bucket = "flexpushdowndb";
  filesystem::path metadataPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/metadata");
  shared_ptr<Catalogue> catalogue = make_shared<Catalogue>("main", metadataPath);

  // AWS client
  shared_ptr<AWSClient> awsClient = make_shared<AWSClient>(
          make_shared<AWSConfig>(normal::aws::S3, 0));

  // read catalogue entry
  const auto &s3CatalogueEntry = S3CatalogueEntryReader::readS3CatalogueEntry(catalogue,
                                                                              s3Bucket,
                                                                              schemaName,
                                                                              awsClient->getS3Client());
  catalogue->putEntry(s3CatalogueEntry);

  // deserialize plan json string into prephysical plan
  shared_ptr<CalcitePlanJsonDeserializer> planDeserializer = make_shared<CalcitePlanJsonDeserializer>(planResult,
                                                                                                      s3CatalogueEntry);
  const auto &prePhysicalPlan = planDeserializer->deserialize();

  // trim unused fields (Calcite trimmer does not trim completely)
  prePhysicalPlan->populateAndTrimProjectColumns();
}

int main() {
  e2eWithoutServer();
  return 0;
}