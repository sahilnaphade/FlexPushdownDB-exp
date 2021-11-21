//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/HashJoinPrePOp.h>

namespace normal::plan::prephysical {

HashJoinPrePOp::HashJoinPrePOp(JoinType joinType,
                               const vector<string> &leftColumnNames,
                               const vector<string> &rightColumnNames) :
  PrePhysicalOp(HashJoin),
  joinType_(joinType),
  leftColumnNames_(leftColumnNames),
  rightColumnNames_(rightColumnNames) {}

string HashJoinPrePOp::getTypeString() {
  return "HashJoinPrePOp";
}

unordered_set<string> HashJoinPrePOp::getUsedColumnNames() {
  unordered_set<string> usedColumnNames = getProjectColumnNames();
  usedColumnNames.insert(leftColumnNames_.begin(), leftColumnNames_.end());
  usedColumnNames.insert(rightColumnNames_.begin(), rightColumnNames_.end());
  return usedColumnNames;
}

}