//
// Created by Yifei Yang on 12/14/21.
//

#include <normal/plan/prephysical/NestedLoopJoinPrePOp.h>

namespace normal::plan::prephysical {

NestedLoopJoinPrePOp::NestedLoopJoinPrePOp(uint id,
                                           JoinType joinType,
                                           const shared_ptr<Expression> &predicate):
  PrePhysicalOp(id, NESTED_LOOP_JOIN),
  joinType_(joinType),
  predicate_(predicate) {}

string NestedLoopJoinPrePOp::getTypeString() {
  return "NestedLoopJoinPrePOp";
}

set<string> NestedLoopJoinPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  if (predicate_) {
    const auto &predicateColumnNames = predicate_->involvedColumnNames();
    usedColumnNames.insert(predicateColumnNames.begin(), predicateColumnNames.end());
  }
  return usedColumnNames;
}

const shared_ptr<Expression> &NestedLoopJoinPrePOp::getPredicate() const {
  return predicate_;
}

JoinType NestedLoopJoinPrePOp::getJoinType() const {
  return joinType_;
}

}
