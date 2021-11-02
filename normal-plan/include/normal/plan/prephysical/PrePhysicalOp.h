//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H

#include <normal/plan/prephysical/PrePOpType.h>
#include <vector>
#include <string>

using namespace std;

namespace normal::plan::prephysical {

class PrePhysicalOp {
public:
  PrePhysicalOp(PrePOpType type);
  virtual ~PrePhysicalOp() = default;

  virtual string getName() = 0;

  PrePOpType getType() const;

  void setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers);

private:
  PrePOpType type_;
  vector<shared_ptr<PrePhysicalOp>> producers_;
  long queryId_{};
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
