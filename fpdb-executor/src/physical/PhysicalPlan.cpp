//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>

namespace fpdb::executor::physical {

PhysicalPlan::PhysicalPlan(const unordered_map<string, shared_ptr<PhysicalOp>> &physicalOps,
                           const string &rootPOpName):
  physicalOps_(physicalOps),
  rootPOpName_(rootPOpName) {}

const unordered_map<string, shared_ptr<PhysicalOp>> &PhysicalPlan::getPhysicalOps() const {
  return physicalOps_;
}

tl::expected<shared_ptr<PhysicalOp>, string> PhysicalPlan::getPhysicalOp(const string &name) const {
  auto physicalOpIt = physicalOps_.find(name);
  if (physicalOpIt != physicalOps_.end()) {
    return physicalOpIt->second;
  } else {
    return tl::make_unexpected(fmt::format("Operator '{}' not found in the physical plan", name));
  }
}

tl::expected<void, string> PhysicalPlan::addAsLast(shared_ptr<PhysicalOp> &op) {
  // check exist
  if (physicalOps_.find(op->name()) != physicalOps_.end()) {
    return tl::make_unexpected(fmt::format("Operator '{}' already exists in the physical plan", op->name()));
  }

  // find root
  auto expRootPOp = getRootPOp();
  if (!expRootPOp.has_value()) {
    return tl::make_unexpected(expRootPOp.error());
  }
  auto rootPOp = *expRootPOp;

  // add before root
  std::vector<std::shared_ptr<PhysicalOp>> rootProducers;
  for (const auto &producerName: rootPOp->producers()) {
    auto producer = physicalOps_.find(producerName)->second;
    rootProducers.emplace_back(producer);
    producer->unProduce(rootPOp);
    rootPOp->unConsume(producer);
  }
  PrePToPTransformerUtil::connectManyToOne(rootProducers, op);
  PrePToPTransformerUtil::connectOneToOne(op, rootPOp);
  
  physicalOps_.emplace(op->name(), op);
  return {};
}

tl::expected<shared_ptr<PhysicalOp>, string> PhysicalPlan::getRootPOp() const {
  auto rootPOpIt = physicalOps_.find(rootPOpName_);
  if (rootPOpIt != physicalOps_.end()) {
    return rootPOpIt->second;
  } else {
    return tl::make_unexpected(fmt::format("Root operator '{}' not found in physical plan", rootPOpName_));
  }
}

tl::expected<void, string> PhysicalPlan::renamePOp(const string oldName, const string newName) {
  // check if new name already exists
  if (physicalOps_.find(newName) != physicalOps_.end()) {
    return tl::make_unexpected(fmt::format("Operator '{}' already exists in the physical plan", newName));
  }

  // get op
  auto expOp = getPhysicalOp(oldName);
  if (!expOp.has_value()) {
    return tl::make_unexpected(expOp.error());
  }
  auto op = *expOp;

  // rename op
  op->setName(newName);
  physicalOps_.erase(oldName);
  physicalOps_.emplace(newName, op);

  // fix relationship
  for (const auto &producer: op->producers()) {
    auto expProducerOp = getPhysicalOp(producer);
    if (!expProducerOp.has_value()) {
      return tl::make_unexpected(expProducerOp.error());
    }
    auto producerOp = *expProducerOp;
    producerOp->reProduce(oldName, newName);
  }
  for (const auto &consumer: op->consumers()) {
    auto expConsumerOp = getPhysicalOp(consumer);
    if (!expConsumerOp.has_value()) {
      return tl::make_unexpected(expConsumerOp.error());
    }
    auto consumerOp = *expConsumerOp;
    consumerOp->reConsume(oldName, newName);
  }

  return {};
}

}
