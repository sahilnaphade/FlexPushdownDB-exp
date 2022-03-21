//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/caf/CAFUtil.h>
#include <utility>

using namespace std;

namespace fpdb::executor::physical {

class PhysicalPlan {
public:
  PhysicalPlan(const vector<shared_ptr<PhysicalOp>> &physicalOps);
  PhysicalPlan() = default;
  PhysicalPlan(const PhysicalPlan&) = default;
  PhysicalPlan& operator=(const PhysicalPlan&) = default;
  ~PhysicalPlan() = default;

  const vector<shared_ptr<PhysicalOp>> &getPhysicalOps() const;
  void addPOp(const shared_ptr<PhysicalOp> &op);

private:
  vector<shared_ptr<PhysicalOp>> physicalOps_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PhysicalPlan& plan) {
    return f.apply(plan.physicalOps_);
  }
};

}

using PhysicalPlanPtr = std::shared_ptr<fpdb::executor::physical::PhysicalPlan>;

CAF_BEGIN_TYPE_ID_BLOCK(PhysicalPlan, fpdb::caf::CAFUtil::PhysicalPlan_first_custom_type_id)
CAF_ADD_TYPE_ID(PhysicalPlan, (fpdb::executor::physical::PhysicalPlan))
CAF_END_TYPE_ID_BLOCK(PhysicalPlan)

namespace caf {
template <>
struct inspector_access<PhysicalPlanPtr> : variant_inspector_access<PhysicalPlanPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
