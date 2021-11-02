//
// Created by Yifei Yang on 11/1/21.
//

#include <normal/plan/prephysical/GroupPrePOp.h>

namespace normal::plan::prephysical {

GroupPrePOp::GroupPrePOp(const vector<string> &groupColumnNames,
                         const vector<string> &aggregateColumnNames,
                         const vector<shared_ptr<AggregatePrePFunction>> &functions) :
   PrePhysicalOp(Group),
   groupColumnNames_(groupColumnNames),
   aggregateColumnNames_(aggregateColumnNames),
   functions_(functions) {}

string GroupPrePOp::getName() {
  return "GroupPrePOp";
}
}
