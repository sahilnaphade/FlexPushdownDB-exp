//
// Created by Yifei Yang on 10/13/21.
//

#ifndef NORMAL_CALCITE_CPP_INCLUDE_CALCITE_H
#define NORMAL_CALCITE_CPP_INCLUDE_CALCITE_H

#include <normal/calcite/CalciteConfig.h>
#include <../gen-cpp/CalciteServer.h>
#include <memory>

using namespace std;

namespace normal::calcite {

class CalciteClient {
public:
  explicit CalciteClient(const shared_ptr<CalciteConfig> &calciteConfig);
  void startClient();
  void startServer();
  void shutdownServer();
  string planQuery(const string& query, const string& schemaName);

private:
  shared_ptr<CalciteConfig> calciteConfig_;
  shared_ptr<CalciteServerClient> calciteServerClient_;
};

}


#endif //NORMAL_CALCITE_CPP_INCLUDE_CALCITE_H
