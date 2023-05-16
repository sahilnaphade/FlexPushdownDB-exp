//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>

namespace fpdb::executor::physical {

BFSPredTransOrder::BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer):
  PredTransOrder(PredTransOrderType::BFS, transformer) {}

void BFSPredTransOrder::orderPredTrans(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // TODO
}
}
