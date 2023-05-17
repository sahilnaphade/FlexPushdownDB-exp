//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>
#include <queue>

namespace fpdb::executor::physical {

BFSPredTransOrder::BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer):
  PredTransOrder(PredTransOrderType::BFS, transformer) {}

void BFSPredTransOrder::orderPredTrans(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // make pred-trans units
  makePTUnits(joinOrigins);

  // order pred-trans by a BFS search
  bfsSearch();

  // TODO
}

void BFSPredTransOrder::makePTUnits(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  for (const auto &joinOrigin: joinOrigins) {
    // transform the join origin ops
    auto upLeftConnPOps = transformer_->transformDfs(joinOrigin->left_);
    auto upRightConnPOps = transformer_->transformDfs(joinOrigin->right_);

    // FIXME: currently only support single-partition-table / single-thread-processing
    if (upLeftConnPOps.size() != 1 || upRightConnPOps.size() != 1) {
      throw std::runtime_error("Currently only support single-partition-table / single-thread-processing");
    }
    auto upLeftConnPOp = upLeftConnPOps[0];
    auto upRightConnPOp = upRightConnPOps[0];

    // cannot transfer on FULL joins
    if (joinOrigin->joinType_ == JoinType::FULL) {
      continue;
    }

    // make or find predicate transfer units
    auto leftPTUnit = std::make_shared<PredTransUnit>(joinOrigin->left_->getId(), upLeftConnPOp);
    auto ptUnitIt = ptUnits_.find(leftPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(leftPTUnit);
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(joinOrigin->right_->getId(), upRightConnPOp);
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(rightPTUnit);
    } else {
      rightPTUnit = *ptUnitIt;
    }

    // add neighbors
    TransferDir leftTransferDir = TransferDir::BOTH;
    TransferDir rightTransferDir = TransferDir::BOTH;
    if (joinOrigin->joinType_ == JoinType::LEFT) {
      leftTransferDir = TransferDir::FORWARD_ONLY;
      rightTransferDir = TransferDir::BACKWARD_ONLY;
    } else if (joinOrigin->joinType_ == JoinType::RIGHT) {
      leftTransferDir = TransferDir::BACKWARD_ONLY;
      rightTransferDir = TransferDir::FORWARD_ONLY;
    }
    leftPTUnit->neighbors_.emplace_back(rightPTUnit, leftTransferDir);
    rightPTUnit->neighbors_.emplace_back(leftPTUnit, rightTransferDir);

    // update root as the ptUnit with the largest table
    double leftRowCount = joinOrigin->left_->getRowCount();
    double rightRowCount = joinOrigin->right_->getRowCount();
    auto largerPTUnit = leftRowCount > rightRowCount ? leftPTUnit : rightPTUnit;
    double largerPTUnitRowCount = leftRowCount > rightRowCount ? leftRowCount : rightRowCount;
    if (rootPTUnit_ == nullptr || largerPTUnitRowCount > rootPTUnitRowCount_) {
      rootPTUnit_ = largerPTUnit;
      rootPTUnitRowCount_ = largerPTUnitRowCount;
    }
  }
}

void BFSPredTransOrder::bfsSearch() {
  std::queue<std::shared_ptr<PredTransUnit>> ptUnitsToVisit;
  ptUnitsToVisit.push(rootPTUnit_);
  ptUnits_.erase(rootPTUnit_);

  // BFS search
  while (!ptUnitsToVisit.empty()) {
    // TODO
  }

  // elements in ptUnits_ should all be visited
  if (!ptUnits_.empty()) {
    throw std::runtime_error("BFS search does not visit all elements in ptUnits_");
  }
}

}
