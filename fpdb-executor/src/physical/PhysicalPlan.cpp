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

const std::string& PhysicalPlan::getRootPOpName() const {
  return rootPOpName_;
}

tl::expected<shared_ptr<PhysicalOp>, string> PhysicalPlan::getPhysicalOp(const string &name) const {
  auto physicalOpIt = physicalOps_.find(name);
  if (physicalOpIt != physicalOps_.end()) {
    return physicalOpIt->second;
  } else {
    return tl::make_unexpected(fmt::format("Operator '{}' not found in the physical plan", name));
  }
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

tl::expected<shared_ptr<PhysicalOp>, string> PhysicalPlan::getLast() {
  auto expRootOp = getRootPOp();
  if (!expRootOp.has_value()) {
    return tl::make_unexpected(expRootOp.error());
  }
  auto rootOpProducers = (*expRootOp)->producers();
  if (rootOpProducers.size() != 1) {
    return tl::make_unexpected("root op does not have exactly 1 producer");
  }
  return getPhysicalOp(*rootOpProducers.begin());
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

tl::expected<void, string> PhysicalPlan::addAsLast(vector<shared_ptr<PhysicalOp>> &ops) {
  // check "#producers of root" = "#op in ops"
  // find root
  auto expRootPOp = getRootPOp();
  if (!expRootPOp.has_value()) {
    return tl::make_unexpected(expRootPOp.error());
  }
  auto rootPOp = *expRootPOp;
  auto rootProducers = rootPOp->producers();
  if (rootProducers.size() != ops.size()) {
    return tl::make_unexpected(fmt::format("size of ops and size of root's producers mismatch: {} vs {}",
                                           rootProducers.size(), ops.size()));
  }

  // add before root
  uint opId = 0;
  for (const auto &producerName: rootProducers) {
    auto op = ops[opId++];
    auto producer = physicalOps_.find(producerName)->second;
    producer->unProduce(rootPOp);
    rootPOp->unConsume(producer);
    PrePToPTransformerUtil::connectOneToOne(producer, op);
    PrePToPTransformerUtil::connectOneToOne(op, rootPOp);
    physicalOps_.emplace(op->name(), op);
  }

  return {};
}

}
