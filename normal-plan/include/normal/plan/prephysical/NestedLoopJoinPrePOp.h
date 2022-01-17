//
// Created by Yifei Yang on 12/14/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_NESTEDLOOPJOINPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_NESTEDLOOPJOINPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/JoinType.h>
#include <normal/expression/gandiva/Expression.h>

namespace normal::plan::prephysical {

class NestedLoopJoinPrePOp: public PrePhysicalOp {

public:
  NestedLoopJoinPrePOp(uint id,
                       JoinType joinType,
                       const shared_ptr<normal::expression::gandiva::Expression> &predicate);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  JoinType getJoinType() const;
  const shared_ptr<normal::expression::gandiva::Expression> &getPredicate() const;

private:
  JoinType joinType_;
  shared_ptr<normal::expression::gandiva::Expression> predicate_;

};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_NESTEDLOOPJOINPREPOP_H
