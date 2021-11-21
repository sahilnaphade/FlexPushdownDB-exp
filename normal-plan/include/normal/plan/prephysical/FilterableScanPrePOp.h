//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/catalogue/Table.h>

using namespace normal::expression::gandiva;
using namespace normal::catalogue;

namespace normal::plan::prephysical {

class FilterableScanPrePOp: public PrePhysicalOp {
public:
  FilterableScanPrePOp(const shared_ptr<Table> &table);

  string getTypeString() override;

  void setPredicate(const shared_ptr<Expression> &predicate);

  unordered_set<string> getUsedColumnNames() override;

private:
  shared_ptr<Expression> predicate_;
  shared_ptr<Table> table_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
