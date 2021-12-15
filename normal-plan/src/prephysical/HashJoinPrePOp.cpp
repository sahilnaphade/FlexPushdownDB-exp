//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/HashJoinPrePOp.h>

namespace normal::plan::prephysical {

HashJoinPrePOp::HashJoinPrePOp(uint id,
                               JoinType joinType,
                               const vector<string> &leftColumnNames,
                               const vector<string> &rightColumnNames) :
  PrePhysicalOp(id, HASH_JOIN),
  joinType_(joinType),
  leftColumnNames_(leftColumnNames),
  rightColumnNames_(rightColumnNames) {}

string HashJoinPrePOp::getTypeString() {
  return "HashJoinPrePOp";
}

set<string> HashJoinPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  usedColumnNames.insert(leftColumnNames_.begin(), leftColumnNames_.end());
  usedColumnNames.insert(rightColumnNames_.begin(), rightColumnNames_.end());
  return usedColumnNames;
}

JoinType HashJoinPrePOp::getJoinType() const {
  return joinType_;
}

const vector<string> &HashJoinPrePOp::getLeftColumnNames() const {
  return leftColumnNames_;
}

const vector<string> &HashJoinPrePOp::getRightColumnNames() const {
  return rightColumnNames_;
}

}