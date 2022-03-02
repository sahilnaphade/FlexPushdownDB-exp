//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreSuperPOp::FPDBStoreSuperPOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId,
                                     const std::shared_ptr<PhysicalPlan> &subPlan):
  PhysicalOp(name, POpType::FPDB_STORE_SUPER, projectColumnNames, nodeId),
  subPlan_(subPlan) {}

void FPDBStoreSuperPOp::onReceive(const Envelope &message) {
  // TODO
}

void FPDBStoreSuperPOp::clear() {
  subPlan_.reset();
}

std::string FPDBStoreSuperPOp::getTypeString() const {
  return "FPDBStoreSuperPOp";
}

std::string FPDBStoreSuperPOp::serialize(bool pretty) {
  PhysicalPlanSerializer serializer(subPlan_);
  auto expSerialization = serializer.serialize(pretty);
  if (!expSerialization.has_value()) {
    ctx()->notifyError(expSerialization.error());
  }
  return *expSerialization;
}

}
