//
// Created by Yifei Yang on 10/30/21.
//

#include <normal/calcite/CalciteConfig.h>
#include <normal/calcite/CalciteClient.h>
#include <memory>
#include <iostream>

using namespace std;
using namespace normal::calcite;

int main() {
  // Create Calcite client
  shared_ptr<CalciteConfig> calciteConfig = parseCalciteConfig();
  CalciteClient calciteClient(calciteConfig);

  // Start Calcite server and client
  calciteClient.startServer();
  calciteClient.startClient();

  // Plan query
  string planResult = calciteClient.planQuery("SELECT COUNT(*) FROM LINEORDER", "ssb-sf1-sortlineorder/csv");
  cout << planResult << endl;

  // Shutdown Calcite server
  calciteClient.shutdownServer();

  return 0;
}