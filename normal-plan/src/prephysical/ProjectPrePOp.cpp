//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/ProjectPrePOp.h>
#include <normal/plan/Util.h>

using namespace normal::plan;

namespace normal::plan::prephysical {
ProjectPrePOp::ProjectPrePOp(uint id,
                             const vector<shared_ptr<Expression>> &exprs,
                             const vector<std::string> &exprNames,
                             const vector<pair<string, string>> &projectColumnNamePairs) :
  PrePhysicalOp(id, PROJECT),
  exprs_(exprs),
  exprNames_(exprNames),
  projectColumnNamePairs_(projectColumnNamePairs) {}

string ProjectPrePOp::getTypeString() {
  return "ProjectPrePOp";
}

set<string> ProjectPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  for (const auto &columnPair: projectColumnNamePairs_) {
    usedColumnNames.emplace(columnPair.first);
  }

  for (const auto &expr: exprs_) {
    const auto involvedColumnNames = expr->involvedColumnNames();
    usedColumnNames.insert(involvedColumnNames.begin(), involvedColumnNames.end());
  }
  return usedColumnNames;
}

const vector<shared_ptr<Expression>> &ProjectPrePOp::getExprs() const {
  return exprs_;
}

const vector<std::string> &ProjectPrePOp::getExprNames() const {
  return exprNames_;
}

const vector<pair<string, string>> &ProjectPrePOp::getProjectColumnNamePairs() const {
  return projectColumnNamePairs_;
}

}
