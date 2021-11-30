//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/PrePhysicalOp.h>

namespace normal::plan::prephysical {

PrePhysicalOp::PrePhysicalOp(PrePOpType type) : type_(type) {}

PrePOpType PrePhysicalOp::getType() const {
  return type_;
}

const vector<shared_ptr<PrePhysicalOp>> &PrePhysicalOp::getProducers() const {
  return producers_;
}

const set<string> &PrePhysicalOp::getProjectColumnNames() const {
  return projectColumnNames_;
}

void PrePhysicalOp::setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers) {
  producers_ = producers;
}

void PrePhysicalOp::setProjectColumnNames(const set<string> &projectColumnNames) {
  projectColumnNames_ = projectColumnNames;
}

}
