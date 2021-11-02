//
// Created by Yifei Yang on 11/1/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_GROUPPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_GROUPPREPOP_H

#include <normal/plan/Util.h>
#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>

namespace normal::plan::prephysical {

class GroupPrePOp: public PrePhysicalOp {
public:
  GroupPrePOp(const vector<string> &groupColumnNames,
              const vector<string> &aggregateColumnNames,
              const vector<shared_ptr<AggregatePrePFunction>> &functions);

  string getName() override;

private:
  vector<string> groupColumnNames_;
  vector<string> aggregateColumnNames_;
  vector<shared_ptr<AggregatePrePFunction>> functions_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_GROUPPREPOP_H
