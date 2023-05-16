//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/SmallToLargePredTransOrder.h>
#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>
#include <fmt/format.h>

namespace fpdb::executor::physical {

void PredTransOrder::orderPredTrans(
        PredTransOrderType type,
        PrePToPTransformerForPredTrans* transformer,
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  std::shared_ptr<PredTransOrder> predTransOrder;
  switch (type) {
    case PredTransOrderType::SMALL_TO_LARGE: {
      predTransOrder = std::make_shared<SmallToLargePredTransOrder>(transformer);
      break;
    }
    case PredTransOrderType::BFS: {
      predTransOrder = std::make_shared<BFSPredTransOrder>(transformer);
      break;
    }
    default: {
      throw std::runtime_error(fmt::format("Unknown PredTransOrderType: '{}'", type));
    }
  }
  predTransOrder->orderPredTrans(joinOrigins);
}

PredTransOrder::PredTransOrder(PredTransOrderType type,
                               PrePToPTransformerForPredTrans* transformer):
  type_(type),
  transformer_(transformer) {}

PredTransOrderType PredTransOrder::getType() const {
  return type_;
}

}
