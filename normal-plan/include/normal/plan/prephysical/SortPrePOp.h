//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H

#include <normal/plan/Util.h>
#include <normal/plan/prephysical/PrePhysicalOp.h>

using namespace std;

namespace normal::plan::prephysical {

class SortPrePOp: public PrePhysicalOp {
public:
  SortPrePOp(const vector<pair<string, FieldDirection>> &sortColumns);

  string getName() override;

private:
  vector<pair<string, FieldDirection>> sortColumns_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_SORTPREPOP_H
