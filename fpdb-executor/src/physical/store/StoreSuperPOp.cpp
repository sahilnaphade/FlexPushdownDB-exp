//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/store/StoreSuperPOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>

namespace fpdb::executor::physical::store {

StoreSuperPOp::StoreSuperPOp(const std::string &name,
                             const std::vector<std::string> &projectColumnNames,
                             int nodeId,
                             const std::shared_ptr<PhysicalPlan> &subPlan):
  PhysicalOp(name, POpType::STORE_SUPER, projectColumnNames, nodeId),
  subPlan_(subPlan) {}

void StoreSuperPOp::onReceive(const Envelope &message) {
  // TODO
}

void StoreSuperPOp::clear() {
  subPlan_.reset();
}

std::string StoreSuperPOp::getTypeString() const {
  return "StoreSuperPOp";
}

std::string StoreSuperPOp::serialize(bool pretty) {
  PhysicalPlanSerializer serializer(subPlan_);
  auto expSerialization = serializer.serialize(pretty);
  if (!expSerialization.has_value()) {
    ctx()->notifyError(expSerialization.error());
  }
  return *expSerialization;
}

}
