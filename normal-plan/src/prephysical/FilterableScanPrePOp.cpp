//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/plan/prephysical/FilterableScanPrePOp.h>

namespace normal::plan::prephysical {

FilterableScanPrePOp::FilterableScanPrePOp(const shared_ptr<Table> &table) :
  PrePhysicalOp(FilterableScan),
  table_(table) {}

void FilterableScanPrePOp::setPredicate(const shared_ptr<Expression> &predicate) {
  predicate_ = predicate;
}

string FilterableScanPrePOp::getTypeString() {
  return "FilterableScanPrePOp";
}

unordered_set<string> FilterableScanPrePOp::getUsedColumnNames() {
  return getProjectColumnNames();
}

}
