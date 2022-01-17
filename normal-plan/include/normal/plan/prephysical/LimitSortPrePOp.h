//
// Created by Yifei Yang on 12/6/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/SortKey.h>

using namespace std;

namespace normal::plan::prephysical {

class LimitSortPrePOp: public PrePhysicalOp {
public:
  LimitSortPrePOp(uint id,
                  int64_t k,
                  const vector<SortKey> &sortKeys);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  int64_t getK() const;
  const vector<SortKey> &getSortKeys() const;

private:
  int64_t k_;
  vector<SortKey> sortKeys_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_LIMITSORTPREPOP_H
