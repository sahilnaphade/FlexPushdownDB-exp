//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/HashJoinPrePOp.h>

namespace normal::plan::prephysical {
HashJoinPrePOp::HashJoinPrePOp(const vector<string> &leftColumnNames,
                               const vector<string> &rightColumnNames) :
  PrePhysicalOp(HashJoin),
  leftColumnNames_(leftColumnNames),
  rightColumnNames_(rightColumnNames) {}

string HashJoinPrePOp::getName() {
  return "HashJoinPrePOp";
}
}