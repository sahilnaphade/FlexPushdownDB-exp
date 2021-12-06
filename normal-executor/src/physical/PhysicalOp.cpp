//
// Created by matt on 5/12/19.
//

#include <normal/executor/physical/PhysicalOp.h>
#include <spdlog/spdlog.h>
#include <cassert>               // for assert
#include <utility>               // for move

namespace normal::executor::physical {

PhysicalOp::PhysicalOp(std::string name,
                       std::string type,
                       std::vector<std::string> projectColumnNames) :
  name_(std::move(name)),
  type_(std::move(type)),
  projectColumnNames_(std::move(projectColumnNames)) {
}

const std::string &PhysicalOp::getType() const {
  return type_;
}

std::string &PhysicalOp::name() {
  return name_;
}

const std::vector<std::string> &PhysicalOp::getProjectColumnNames() const {
  return projectColumnNames_;
}

void PhysicalOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  consumers_.emplace(op->name(), op->name());
}

void PhysicalOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  producers_.emplace(op->name(), op->name());
}

std::map<std::string, std::string> PhysicalOp::consumers() {
  return consumers_;
}

std::map<std::string, std::string> PhysicalOp::producers() {
  return producers_;
}

std::shared_ptr<POpContext> PhysicalOp::ctx() {
  return opContext_;
}

void PhysicalOp::create(const std::shared_ptr<POpContext>& ctx) {
  assert (ctx);
  SPDLOG_DEBUG("Creating operator  |  name: '{}'", this->name_);
  opContext_ = ctx;
}

void PhysicalOp::setName(const std::string &Name) {
  name_ = Name;
}

void PhysicalOp::setProjectColumnNames(const std::vector<std::string> &projectColumnNames) {
  projectColumnNames_ = projectColumnNames;
}

void PhysicalOp::destroyActor() {
  opContext_->destroyActorHandles();
}

long PhysicalOp::getQueryId() const {
  return queryId_;
}

void PhysicalOp::setQueryId(long queryId) {
  queryId_ = queryId;
}

} // namespace
