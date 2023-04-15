//
// Created by Yifei Yang on 11/8/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/catalogue/Table.h>

using namespace fpdb::catalogue;

namespace fpdb::plan::prephysical {

class FilterableScanPrePOp: public PrePhysicalOp {
public:
  FilterableScanPrePOp(uint id, const shared_ptr<Table> &table, double rowCount);

  string getTypeString() override;
  set<string> getUsedColumnNames() override;
  void setProjectColumnNames(const set<string> &projectColumnNames) override;

  const shared_ptr<fpdb::expression::gandiva::Expression> &getPredicate() const;
  const shared_ptr<Table> &getTable() const;
  double getRowCount() const;
  void setPredicate(const shared_ptr<fpdb::expression::gandiva::Expression> &predicate);
  void setRowCount(double rowCount);

private:
  shared_ptr<fpdb::expression::gandiva::Expression> predicate_;
  shared_ptr<Table> table_;
  double rowCount_;   // used when predicate transfer is enabled
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
