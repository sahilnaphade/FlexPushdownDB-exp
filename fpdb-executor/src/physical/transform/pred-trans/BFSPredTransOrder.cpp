//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <queue>

namespace fpdb::executor::physical {

BFSPredTransOrder::BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer,
                                     bool isYannakakis):
  PredTransOrder(PredTransOrderType::BFS, transformer),
  isYannakakis_(isYannakakis) {}

void BFSPredTransOrder::orderPredTrans(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // make pred-trans units
  makePTUnits(joinOrigins);

  // order pred-trans by a BFS search
  bfsSearch();

  // connect pairs ptUnits by pred-trans operators (i.e. bloom filter / semi-join)
  connectPTUnits();

  // update transformation results
  updateTransRes();

#if SHOW_DEBUG_METRICS == true
  // collect predicate transfer metrics
  for (const auto &ptUnit: ptUnits_) {
    auto currConnOp = ptUnit->base_->currUpConnOp_;
    uint prePOpId = ptUnit->base_->prePOpId_;
    currConnOp->setCollPredTransMetrics(prePOpId, metrics::PredTransMetrics::PTMetricsUnitType::PRED_TRANS);
  }
#endif
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
      origUpConnOpToPTUnit_[upLeftConnPOp->name()] = leftPTUnit->base_;
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(joinOrigin->right_->getId(), upRightConnPOp);
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(rightPTUnit);
      origUpConnOpToPTUnit_[upRightConnPOp->name()] = rightPTUnit->base_;
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
    leftPTUnit->neighbors_.emplace_back(std::make_shared<PredTransNeighbor>(
            rightPTUnit, leftTransferDir, joinOrigin->leftColumns_, joinOrigin->rightColumns_));
    rightPTUnit->neighbors_.emplace_back(std::make_shared<PredTransNeighbor>(
            leftPTUnit, rightTransferDir, joinOrigin->rightColumns_, joinOrigin->leftColumns_));

    if (isYannakakis_) {
      // Yannakakis does not specify the root so we just pick the first one
      if (rootPTUnit_ == nullptr) {
        rootPTUnit_ = leftPTUnit;
      }
    } else {
      // in pred-trans, we specify the root as the ptUnit with the largest table
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
}

void BFSPredTransOrder::bfsSearch() {
  std::queue<std::shared_ptr<PredTransUnit>> ptUnitsToVisit;
  ptUnitsToVisit.push(rootPTUnit_);
  ptUnits_.erase(rootPTUnit_);

  // BFS search
  while (!ptUnitsToVisit.empty()) {
    const auto &ptUnit = ptUnitsToVisit.front();
    ptUnitsToVisit.pop();
    for (const auto &neighbor: ptUnit->neighbors_) {
      auto nextPtUnit = neighbor->ptUnit_.lock();
      if (ptUnits_.find(nextPtUnit) == ptUnits_.end()) {
        // this node has been visited
        continue;
      }
      // get whether the forward or backward transfer is doable, or both are
      bool forward = neighbor->transferDir_ == TransferDir::BOTH ||
              neighbor->transferDir_ == TransferDir::FORWARD_ONLY;
      bool backward = neighbor->transferDir_ == TransferDir::BOTH ||
              neighbor->transferDir_ == TransferDir::BACKWARD_ONLY;
      forwardOrder_.push(std::make_shared<BFSPredTransPair>(nextPtUnit, ptUnit,
                                                            neighbor->rightColumns_, neighbor->leftColumns_,
                                                            backward, forward));
      ptUnitsToVisit.push(nextPtUnit);
      ptUnits_.erase(nextPtUnit);
    }
  }

  // elements in ptUnits_ should all be visited
  if (!ptUnits_.empty()) {
    throw std::runtime_error("BFS search does not visit all elements in ptUnits_");
  }
}

void BFSPredTransOrder::connectPTUnits() {
  connectPTUnits(true);     // forward
  connectPTUnits(false);    // backward
}

void BFSPredTransOrder::connectPTUnits(bool isForward) {
  const auto &orderToVisit = isForward ? forwardOrder_ : backwardOrder_;
  std::string dirNameTag = isForward ? "F" : "B";
  while (!orderToVisit.empty()) {
    const auto &ptPair = orderToVisit.top();
    if (ptPair->forward_) {
      join::HashJoinPredicate hashJoinPredicate(ptPair->srcColumns_, ptPair->tgtColumns_);
      const auto &hashJoinPredicateStr = hashJoinPredicate.toString();
      uint ptOpId = ptOpIdGen_.fetch_add(1);
      if (isYannakakis_) {
        // semi-join for Yannakakis
        std::shared_ptr<PhysicalOp> hashJoin = std::make_shared<join::HashJoinArrowPOp>(
                fmt::format("HashJoinArrow({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                ptPair->tgtPTUnit_->base_->origUpConnOp_->getProjectColumnNames(),
                0,
                hashJoinPredicate,
                JoinType::RIGHT_SEMI);
        // connect and add ops
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)
                ->addBuildProducer(ptPair->srcPTUnit_->base_->currUpConnOp_);
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)
                ->addProbeProducer(ptPair->tgtPTUnit_->base_->currUpConnOp_);
        PrePToPTransformerUtil::addPhysicalOps({hashJoin}, transformer_->physicalOps_);
        // update currUpConnOp for ptUnit
        ptPair->tgtPTUnit_->base_->currUpConnOp_ = hashJoin;
      } else {
        // bloom filter for pred-trans
        std::shared_ptr<PhysicalOp> bfCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                std::vector<std::string>{},
                0,
                ptPair->srcColumns_);
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                std::vector<std::string>{},
                0,
                ptPair->tgtColumns_);
        // connect and add ops
        std::static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bfCreate)->addBloomFilterUsePOp(bfUse);
        bfUse->consume(bfCreate);
        PrePToPTransformerUtil::connectOneToOne(ptPair->srcPTUnit_->base_->currUpConnOp_, bfCreate);
        PrePToPTransformerUtil::connectOneToOne(ptPair->tgtPTUnit_->base_->currUpConnOp_, bfUse);
        PrePToPTransformerUtil::addPhysicalOps({bfCreate, bfUse}, transformer_->physicalOps_);
        // update currUpConnOp for ptUnit
        ptPair->tgtPTUnit_->base_->currUpConnOp_ = bfUse;
      }
    }

    // also construct the reversed order during the forward visit, for backward transfer
    if (isForward) {
      backwardOrder_.push(std::make_shared<BFSPredTransPair>(ptPair->tgtPTUnit_, ptPair->srcPTUnit_,
                                                             ptPair->tgtColumns_, ptPair->srcColumns_,
                                                             ptPair->backward_, ptPair->forward_));
    }
  }
}

}
