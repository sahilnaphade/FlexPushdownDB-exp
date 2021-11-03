//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/prephysical/AggregatePrePOp.h>

namespace normal::plan::prephysical {

AggregatePrePOp::AggregatePrePOp(const vector<string> &aggOutputColumnNames,
                                 const vector<shared_ptr<AggregatePrePFunction>> &functions):
  PrePhysicalOp(Aggregate),
  aggOutputColumnNames_(aggOutputColumnNames),
  functions_(functions) {}

string AggregatePrePOp::getName() {
  return "AggregatePrePOp";
}
}