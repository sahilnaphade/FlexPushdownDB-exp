//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/expression/gandiva/Expression.h>

using namespace normal::expression::gandiva;

namespace normal::plan::prephysical {

class FilterPrePOp: public PrePhysicalOp {
public:
  FilterPrePOp(uint id, const shared_ptr<Expression> &predicate);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  const shared_ptr<Expression> &getPredicate() const;

private:
  shared_ptr<Expression> predicate_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERPREPOP_H
