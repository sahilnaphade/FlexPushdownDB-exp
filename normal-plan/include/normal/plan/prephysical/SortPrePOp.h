//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H

#include <normal/plan/prephysical/FieldDirection.h>
#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <arrow/compute/api.h>

using namespace std;

namespace normal::plan::prephysical {

class SortPrePOp: public PrePhysicalOp {
public:
  SortPrePOp(const arrow::compute::SortOptions &sortOptions);

  string getTypeString() override;

  unordered_set<string> getUsedColumnNames() override;

  const arrow::compute::SortOptions &getSortOptions() const;

private:
  arrow::compute::SortOptions sortOptions_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
