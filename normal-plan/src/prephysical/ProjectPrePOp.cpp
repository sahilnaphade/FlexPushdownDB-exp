//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/ProjectPrePOp.h>

namespace normal::plan::prephysical {
ProjectPrePOp::ProjectPrePOp(const vector<shared_ptr<Expression>> &exprs,
                             const vector<std::string> &exprNames,
                             const unordered_map<string, string> &columnRenames) :
  PrePhysicalOp(PROJECT),
  exprs_(exprs),
  exprNames_(exprNames),
  columnRenames_(columnRenames) {}

string ProjectPrePOp::getTypeString() {
  return "ProjectPrePOp";
}

set<string> ProjectPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
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

const unordered_map<string, string> &ProjectPrePOp::getColumnRenames() const {
  return columnRenames_;
}

}
