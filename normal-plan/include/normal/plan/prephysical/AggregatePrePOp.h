//
// Created by Yifei Yang on 11/1/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_AGGREGATEPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_AGGREGATEPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>

namespace normal::plan::prephysical {

class AggregatePrePOp: public PrePhysicalOp {
public:
  AggregatePrePOp(uint id,
                  const vector<string> &aggOutputColumnNames,
                  const vector<shared_ptr<AggregatePrePFunction>> &functions);

  const vector<string> &getAggOutputColumnNames() const;
  const vector<shared_ptr<AggregatePrePFunction>> &getFunctions() const;

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

private:
  vector<string> aggOutputColumnNames_;
  vector<shared_ptr<AggregatePrePFunction>> functions_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_AGGREGATEPREPOP_H
