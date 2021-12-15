//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/plan/prephysical/FilterableScanPrePOp.h>

namespace normal::plan::prephysical {

FilterableScanPrePOp::FilterableScanPrePOp(uint id, const shared_ptr<Table> &table) :
  PrePhysicalOp(id, FILTERABLE_SCAN),
  table_(table) {}

string FilterableScanPrePOp::getTypeString() {
  return "FilterableScanPrePOp";
}

set<string> FilterableScanPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  if (predicate_) {
    const auto &predicateColumnNames = predicate_->involvedColumnNames();
    usedColumnNames.insert(predicateColumnNames.begin(), predicateColumnNames.end());
  }
  return usedColumnNames;
}

const shared_ptr<Expression> &FilterableScanPrePOp::getPredicate() const {
  return predicate_;
}

const shared_ptr<Table> &FilterableScanPrePOp::getTable() const {
  return table_;
}

void FilterableScanPrePOp::setPredicate(const shared_ptr<Expression> &predicate) {
  predicate_ = predicate;
}

}
