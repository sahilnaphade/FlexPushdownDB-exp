//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/catalogue/Table.h>

using namespace normal::catalogue;

namespace normal::plan::prephysical {

class FilterableScanPrePOp: public PrePhysicalOp {
public:
  FilterableScanPrePOp(uint id, const shared_ptr<Table> &table);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  const shared_ptr<normal::expression::gandiva::Expression> &getPredicate() const;
  const shared_ptr<Table> &getTable() const;

  void setPredicate(const shared_ptr<normal::expression::gandiva::Expression> &predicate);


private:
  shared_ptr<normal::expression::gandiva::Expression> predicate_;
  shared_ptr<Table> table_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
