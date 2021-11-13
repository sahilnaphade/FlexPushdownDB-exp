//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/plan/prephysical/FilterPrePOp.h>

namespace normal::plan::prephysical {

FilterPrePOp::FilterPrePOp(const shared_ptr<Expression> &predicate) :
  PrePhysicalOp(Filter),
  predicate_(predicate) {}

string FilterPrePOp::getName() {
  return "FilterPrePOp";
}

unordered_set<string> FilterPrePOp::getUsedColumnNames() {
  unordered_set<string> usedColumnNames = getProjectColumnNames();
  const auto &predicateColumnNames = predicate_->involvedColumnNames();
  usedColumnNames.insert(predicateColumnNames->begin(), predicateColumnNames->end());
  return usedColumnNames;
}

}
