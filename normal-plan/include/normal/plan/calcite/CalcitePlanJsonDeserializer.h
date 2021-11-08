//
// Created by Yifei Yang on 11/1/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/SortPrePOp.h>
#include <normal/plan/prephysical/AggregatePrePOp.h>
#include <normal/plan/prephysical/GroupPrePOp.h>
#include <normal/plan/prephysical/ProjectPrePOp.h>
#include <normal/plan/prephysical/HashJoinPrePOp.h>
#include <nlohmann/json.hpp>
#include <string>

using json = nlohmann::json;
using namespace std;
using namespace normal::expression::gandiva;

namespace normal::plan::calcite {

class CalcitePlanJsonDeserializer {
public:
  static void deserialize(const string &planJsonString);

private:
  static shared_ptr<prephysical::PrePhysicalOp> deserializeDfs(json &jObj);
  static vector<shared_ptr<prephysical::PrePhysicalOp>> deserializeProducers(const json &jObj);

  static shared_ptr<Expression> deserializeExpression(const json &jObj);
  static pair<vector<string>, vector<string>> deserializeHashJoinCondition(const json &jObj);

  static shared_ptr<prephysical::SortPrePOp> deserializeSort(const json &jObj);
  static shared_ptr<prephysical::PrePhysicalOp> deserializeAggregateOrGroup(json &jObj);
  static shared_ptr<prephysical::PrePhysicalOp> deserializeProject(const json &jObj);
  static shared_ptr<prephysical::HashJoinPrePOp> deserializeHashJoin(const json &jObj);
};


}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_CALCITEPLANJSONDESERIALIZER_H
