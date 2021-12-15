//
// Created by Yifei Yang on 12/6/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <arrow/compute/api.h>

using namespace std;

namespace normal::plan::prephysical {

class LimitSortPrePOp: public PrePhysicalOp {
public:
  LimitSortPrePOp(uint id, const arrow::compute::SelectKOptions &selectKOptions);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  const arrow::compute::SelectKOptions &getSelectKOptions() const;

private:
  arrow::compute::SelectKOptions selectKOptions_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H
