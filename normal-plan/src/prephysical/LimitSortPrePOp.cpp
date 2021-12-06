//
// Created by Yifei Yang on 12/6/21.
//

#include <normal/plan/prephysical/LimitSortPrePOp.h>
#include <normal/plan/prephysical/PrePOpType.h>

namespace normal::plan::prephysical {

LimitSortPrePOp::LimitSortPrePOp(const arrow::compute::SelectKOptions &selectKOptions):
  PrePhysicalOp(LIMIT_SORT),
  selectKOptions_(selectKOptions) {}

string LimitSortPrePOp::getTypeString() {
  return "LimitSortPrePOp";
}

set<string> LimitSortPrePOp::getUsedColumnNames() {
  return getProjectColumnNames();
}

const arrow::compute::SelectKOptions &LimitSortPrePOp::getSelectKOptions() const {
  return selectKOptions_;
}

}
