//
// Created by Yifei Yang on 4/21/23.
//

#include <fpdb/plan/prephysical/Util.h>

namespace fpdb::plan::prephysical {

std::optional<uint> Util::traceScanOriginWithNoJoinInPath(const std::shared_ptr<PrePhysicalOp> &op) {
  if (op->getType() == PrePOpType::FILTERABLE_SCAN || op->getType() == PrePOpType::SEPARABLE_SUPER) {
    return op->getId();
  } else if (op->getType() == PrePOpType::HASH_JOIN || op->getType() == PrePOpType::NESTED_LOOP_JOIN) {
    return std::nullopt;
  } else {
    return traceScanOriginWithNoJoinInPath(op->getProducers()[0]);
  }
}

}
