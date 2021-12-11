//
// Created by Yifei Yang on 11/1/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H

#include <normal/plan/prephysical/PrePhysicalPlan.h>
#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/SortPrePOp.h>
#include <normal/plan/prephysical/LimitSortPrePOp.h>
#include <normal/plan/prephysical/AggregatePrePOp.h>
#include <normal/plan/prephysical/GroupPrePOp.h>
#include <normal/plan/prephysical/ProjectPrePOp.h>
#include <normal/plan/prephysical/HashJoinPrePOp.h>
#include <normal/plan/prephysical/FilterPrePOp.h>
#include <normal/plan/prephysical/FilterableScanPrePOp.h>
#include <normal/catalogue/CatalogueEntry.h>
#include <nlohmann/json.hpp>
#include <string>

using namespace normal::plan::prephysical;
using namespace normal::expression::gandiva;
using json = nlohmann::json;
using namespace std;

namespace normal::plan::calcite {

class CalcitePlanJsonDeserializer {
public:
  CalcitePlanJsonDeserializer(string planJsonString, const shared_ptr<CatalogueEntry> &catalogueEntry);

  shared_ptr<PrePhysicalPlan> deserialize();

private:
  shared_ptr<PrePhysicalOp> deserializeDfs(json &jObj);
  vector<shared_ptr<PrePhysicalOp>> deserializeProducers(const json &jObj);

  shared_ptr<Expression> deserializeInputRef(const json &jObj);
  shared_ptr<Expression> deserializeLiteral(const json &jObj);
  shared_ptr<Expression> deserializeOperation(const json &jObj);
  shared_ptr<Expression> deserializeAndOrNotOperation(const string &opName, const json &jObj);
  shared_ptr<Expression> deserializeBinaryOperation(const string &opName, const json &jObj);
  shared_ptr<Expression> deserializeInOperation(const json &jObj);
  shared_ptr<Expression> deserializeCaseOperation(const json &jObj);
  shared_ptr<Expression> deserializeExpression(const json &jObj);

  pair<vector<string>, vector<string>> deserializeHashJoinCondition(const json &jObj);
  vector<arrow::compute::SortKey> deserializeSortKeys(const json &jObj);

  shared_ptr<SortPrePOp> deserializeSort(const json &jObj);
  shared_ptr<LimitSortPrePOp> deserializeLimitSort(const json &jObj);
  shared_ptr<PrePhysicalOp> deserializeAggregateOrGroup(json &jObj);
  shared_ptr<PrePhysicalOp> deserializeProject(const json &jObj);
  shared_ptr<HashJoinPrePOp> deserializeHashJoin(const json &jObj);
  shared_ptr<PrePhysicalOp> deserializeFilterOrFilterableScan(const json &jObj);
  shared_ptr<FilterableScanPrePOp> deserializeTableScan(const json &jObj);

  string planJsonString_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
};


}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H
