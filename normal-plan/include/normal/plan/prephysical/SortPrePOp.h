//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/plan/prephysical/SortKey.h>

using namespace std;

namespace normal::plan::prephysical {

class SortPrePOp: public PrePhysicalOp {
public:
  SortPrePOp(uint id, const vector<SortKey> &sortKeys);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  const vector<SortKey> &getSortKeys() const;

private:
  vector<SortKey> sortKeys_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
