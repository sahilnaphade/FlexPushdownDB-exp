//
// Created by Yifei Yang on 11/20/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALPLAN_H

#include <normal/executor/physical/PhysicalOp.h>
#include <utility>

using namespace std;

namespace normal::executor::physical {

class PhysicalPlan {
public:
  PhysicalPlan(const vector<shared_ptr<PhysicalOp>> &physicalOps);

  const vector<shared_ptr<PhysicalOp>> &getPhysicalOps() const;

private:
  vector<shared_ptr<PhysicalOp>> physicalOps_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
