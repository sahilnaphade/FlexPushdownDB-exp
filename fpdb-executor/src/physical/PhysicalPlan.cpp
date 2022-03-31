//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>

namespace fpdb::executor::physical {

PhysicalPlan::PhysicalPlan(const vector<shared_ptr<PhysicalOp>> &physicalOps)
        : physicalOps_(physicalOps) {}

const vector<shared_ptr<PhysicalOp>> &PhysicalPlan::getPhysicalOps() const {
  return physicalOps_;
}

tl::expected<void, string> PhysicalPlan::addAsLast(shared_ptr<PhysicalOp> &newOp) {
  // find collate
  std::optional<std::shared_ptr<PhysicalOp>> collatePOp;
  std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> opMap;
  for (const auto &op: physicalOps_) {
    opMap.emplace(op->name(), op);
    if (op->getType() == POpType::COLLATE) {
      collatePOp = op;
    }
  }

  if (!collatePOp.has_value()) {
    return tl::make_unexpected("CollatePOp not found in physical plan");
  }

  // add before collate
  std::vector<std::shared_ptr<PhysicalOp>> collateProducers;
  for (const auto &producerName: (*collatePOp)->producers()) {
    auto producer = opMap.find(producerName)->second;
    collateProducers.emplace_back(producer);
    producer->unProduce(*collatePOp);
    (*collatePOp)->unConsume(producer);
  }
  PrePToPTransformerUtil::connectManyToOne(collateProducers, newOp);
  PrePToPTransformerUtil::connectOneToOne(newOp, *collatePOp);
  
  physicalOps_.emplace_back(newOp);
  return {};
}

tl::expected<shared_ptr<PhysicalOp>, string> PhysicalPlan::getCollatePOp() const {
  unordered_map<string, shared_ptr<PhysicalOp>> opMap;
  for (const auto &op: physicalOps_) {
    if (op->getType() == POpType::COLLATE) {
      return op;
    }
  }
  return tl::make_unexpected("CollatePOp not found in physical plan");
}

}
