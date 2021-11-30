//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_CLIENT_H
#define NORMAL_FRONTEND_CLIENT_H

#include <normal/frontend/ExecConfig.h>
#include <normal/executor/Executor.h>
#include <normal/executor/physical/PhysicalPlan.h>
#include <normal/calcite/CalciteClient.h>

using namespace normal::executor;
using namespace normal::executor::physical;
using namespace normal::calcite;
using namespace std::filesystem;

namespace normal::frontend {

class Client {

public:
  explicit Client();

  static path getDefaultMetadataPath();
  static string getDefaultCatalogueName();

  string start();
  string stop();
  string restart();

  string executeQuery(const string &query);
  string executeQueryFile(const string &queryFilePath);

private:
  shared_ptr<CatalogueEntry> getCatalogueEntry(const string &schemaName);
  shared_ptr<PhysicalPlan> plan(const string &query, const shared_ptr<CatalogueEntry> &catalogueEntry);
  pair<shared_ptr<TupleSet>, long> execute(const shared_ptr<PhysicalPlan> &physicalPlan);

  // catalogue
  shared_ptr<Catalogue> catalogue_;

  // AWS client
  shared_ptr<AWSClient> awsClient_;

  // calcite client
  shared_ptr<CalciteClient> calciteClient_;

  // config parameters
  shared_ptr<ExecConfig> execConfig_;

  // executor
  shared_ptr<Executor> executor_;
};

}

#endif //NORMAL_FRONTEND_CLIENT_H
