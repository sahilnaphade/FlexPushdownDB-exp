//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/prephysical/GroupPrePOp.h>

namespace normal::plan::prephysical {

GroupPrePOp::GroupPrePOp(const vector<string> &groupColumnNames,
                         const vector<string> &aggOutputColumnNames,
                         const vector<shared_ptr<AggregatePrePFunction>> &functions) :
   PrePhysicalOp(Group),
   groupColumnNames_(groupColumnNames),
   aggOutputColumnNames_(aggOutputColumnNames),
   functions_(functions) {}

string GroupPrePOp::getName() {
  return "GroupPrePOp";
}

unordered_set<string> GroupPrePOp::getUsedColumnNames() {
  unordered_set<string> usedColumnNames(groupColumnNames_.begin(), groupColumnNames_.end());
  for (const auto &function: functions_) {
    const auto involvedColumnNames = function->getExpression()->involvedColumnNames();
    usedColumnNames.insert(involvedColumnNames->begin(), involvedColumnNames->end());
  }
  return usedColumnNames;
}
}
