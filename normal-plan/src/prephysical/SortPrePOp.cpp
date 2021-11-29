//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/SortPrePOp.h>
#include <normal/plan/prephysical/PrePOpType.h>

using namespace std;

namespace normal::plan::prephysical {

SortPrePOp::SortPrePOp(const arrow::compute::SortOptions &sortOptions) :
  PrePhysicalOp(SORT),
  sortOptions_(sortOptions) {}

string SortPrePOp::getTypeString() {
  return "SortPrePOp";
}

set<string> SortPrePOp::getUsedColumnNames() {
  return getProjectColumnNames();
}

const arrow::compute::SortOptions &SortPrePOp::getSortOptions() const {
  return sortOptions_;
}

}
