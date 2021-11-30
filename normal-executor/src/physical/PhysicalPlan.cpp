//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/PhysicalPlan.h>

namespace normal::executor::physical {

PhysicalPlan::PhysicalPlan(const vector<shared_ptr<PhysicalOp>> &physicalOps)
        : physicalOps_(physicalOps) {}

const vector<shared_ptr<PhysicalOp>> &PhysicalPlan::getPhysicalOps() const {
  return physicalOps_;
}

}
