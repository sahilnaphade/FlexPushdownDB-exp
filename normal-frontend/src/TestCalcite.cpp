//
// Created by Yifei Yang on 10/30/21.
//

#include <normal/calcite/CalciteConfig.h>
#include <normal/calcite/CalciteClient.h>
#include <normal/plan/calcite/CalcitePlanJsonDeserializer.h>
#include <normal/util/Util.h>
#include <memory>
#include <iostream>
#include <filesystem>

using namespace std;
using namespace normal::calcite;
using namespace normal::plan::calcite;
using namespace normal::util;

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
  string schemaName = "ssb-sf1-sortlineorder/csv";
  string planResult = calciteClient.planQuery(query, schemaName);
  cout << planResult << endl;
  CalcitePlanJsonDeserializer::deserialize(planResult);
}

int main() {
  e2eWithoutServer();
  return 0;
}