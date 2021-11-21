//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/ProjectPrePOp.h>

namespace normal::plan::prephysical {
ProjectPrePOp::ProjectPrePOp(const vector<shared_ptr<Expression>> &exprs) :
  PrePhysicalOp(Project),
  exprs_(exprs) {}

string ProjectPrePOp::getTypeString() {
  return "ProjectPrePOp";
}

unordered_set<string> ProjectPrePOp::getUsedColumnNames() {
  unordered_set<string> usedColumnNames = getProjectColumnNames();
  for (const auto &expr: exprs_) {
    const auto involvedColumnNames = expr->involvedColumnNames();
    usedColumnNames.insert(involvedColumnNames->begin(), involvedColumnNames->end());
  }
  return usedColumnNames;
}

const vector<shared_ptr<Expression>> &ProjectPrePOp::getExprs() const {
  return exprs_;
}

}
