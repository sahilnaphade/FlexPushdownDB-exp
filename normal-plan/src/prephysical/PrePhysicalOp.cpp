//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/PrePhysicalOp.h>

namespace normal::plan::prephysical {

PrePhysicalOp::PrePhysicalOp(PrePOpType type) : type_(type) {}

PrePOpType PrePhysicalOp::getType() const {
  return type_;
}

void PrePhysicalOp::setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers) {
  producers_ = producers;
}

}
