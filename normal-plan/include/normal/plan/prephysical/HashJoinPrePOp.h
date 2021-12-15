//
// Created by Yifei Yang on 11/7/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_HASHJOINPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_HASHJOINPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/JoinType.h>

namespace normal::plan::prephysical {

class HashJoinPrePOp: public PrePhysicalOp {
public:
  HashJoinPrePOp(uint id,
                 JoinType joinType,
                 const vector<string> &leftColumnNames,
                 const vector<string> &rightColumnNames);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  JoinType getJoinType() const;
  const vector<string> &getLeftColumnNames() const;
  const vector<string> &getRightColumnNames() const;

private:
  JoinType joinType_;
  vector<string> leftColumnNames_;
  vector<string> rightColumnNames_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_HASHJOINPREPOP_H
