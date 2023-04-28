//
// Created by Yifei Yang on 4/11/23.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerForPredTrans.h>
#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/prephysical/Util.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <queue>

namespace fpdb::executor::physical {

PrePToPTransformerForPredTrans::PrePToPTransformerForPredTrans(
        const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
        const shared_ptr<CatalogueEntry> &catalogueEntry,
        const shared_ptr<ObjStoreConnector> &objStoreConnector,
        const shared_ptr<Mode> &mode,
        int parallelDegree,
        int numNodes):
  PrePToPTransformer(prePhysicalPlan, catalogueEntry, objStoreConnector, mode, parallelDegree, numNodes) {
  type_ = PrePToPTransformerType::PRED_TRANS;
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transform(
        const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
        const shared_ptr<CatalogueEntry> &catalogueEntry,
        const shared_ptr<ObjStoreConnector> &objStoreConnector,
        const shared_ptr<Mode> &,
        int parallelDegree,
        int numNodes) {
  // currently pushdown is not supported
  PrePToPTransformerForPredTrans transformer(prePhysicalPlan, catalogueEntry, objStoreConnector,
                                             Mode::pullupMode(), parallelDegree, numNodes);
  return transformer.transform();
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transform() {
  // Phase 1
  transformPredTrans();

  // Phase 2
  auto upConnOps = transformExec();

#if SHOW_DEBUG_METRICS == true
  // collect predicate transfer metrics
  for (const auto &ptUnit: ptUnits_) {
    auto currConnOp = ptUnit->currUpConnOp_;
    uint prePOpId = ptUnit->prePOpId_;
    currConnOp->setCollPredTransMetrics(prePOpId, metrics::PredTransMetrics::PTMetricsUnitType::PRED_TRANS);
  }
#endif

  // make the final plan
  std::shared_ptr<PhysicalOp> collateOp = std::make_shared<collate::CollatePOp>(
          "Collate",
          ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()),
          0);
  PrePToPTransformerUtil::connectManyToOne(upConnOps, collateOp);
  PrePToPTransformerUtil::addPhysicalOps({collateOp}, physicalOps_);
  return std::make_shared<PhysicalPlan>(physicalOps_, collateOp->name());
}

void PrePToPTransformerForPredTrans::transformPredTrans() {
  // extract base table joins
  auto joinOrigins = JoinOriginTracer::trace(prePhysicalPlan_);

  // create bloom filter ops (both forward and backward)
  makeBloomFilterOps(joinOrigins);

  // connect forward bloom filter ops
  connectFwBloomFilterOps();

  // connect backward bloom filter ops
  connectBwBloomFilterOps();
}

std::vector<std::shared_ptr<PhysicalOp>>
PrePToPTransformerForPredTrans::transformDfs(const std::shared_ptr<PrePhysicalOp> &prePOp) {
  // check if this prepOp has already been visited, since one may be visited multiple times
  auto transformResIt = prePOpToTransRes_.find(prePOp->getId());
  if (transformResIt != prePOpToTransRes_.end()) {
    return transformResIt->second;
  }

  // transform calling the base-class impl
  auto transRes = PrePToPTransformer::transformDfs(prePOp);

  // save transformation results
  prePOpToTransRes_[prePOp->getId()] = transRes;
  return transRes;
}

void PrePToPTransformerForPredTrans::makeBloomFilterOps(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  for (const auto &joinOrigin: joinOrigins) {
    // transform the join origin ops
    auto upLeftConnPOps = transformDfs(joinOrigin->left_);
    auto upRightConnPOps = transformDfs(joinOrigin->right_);

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
      origUpConnOpToPTUnit_[upLeftConnPOp->name()] = leftPTUnit;
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(joinOrigin->right_->getId(), upRightConnPOp);
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(rightPTUnit);
      origUpConnOpToPTUnit_[upRightConnPOp->name()] = rightPTUnit;
    } else {
      rightPTUnit = *ptUnitIt;
    }

    // make bloom filter ops
    join::HashJoinPredicate hashJoinPredicate(joinOrigin->leftColumns_, joinOrigin->rightColumns_);
    const auto &hashJoinPredicateStr = hashJoinPredicate.toString();
    uint bfId = bfIdGen_.fetch_add(1);

    // forward bloom filter, blocked by right joins
    if (joinOrigin->joinType_ != JoinType::RIGHT) {
      auto fwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate(F)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->leftColumns_);
      auto fwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse(F)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->rightColumns_);
      fwBFCreate->addBloomFilterUsePOp(fwBFUse);
      fwBFUse->consume(fwBFCreate);

      // add ops
      PrePToPTransformerUtil::addPhysicalOps({fwBFCreate, fwBFUse}, physicalOps_);

      // make predicate transfer graph nodes
      auto fwPTGraphNode = std::make_shared<PredTransGraphNode>(fwBFCreate, fwBFUse, leftPTUnit, rightPTUnit);
      fwPTGraphNodes_.emplace(fwPTGraphNode);
      leftPTUnit->fwOutPTNodes_.emplace_back(fwPTGraphNode);
      ++rightPTUnit->numFwBFUseToVisit_;
    }

    // backward bloom filter, blocked by left joins
    if (joinOrigin->joinType_ != JoinType::LEFT) {
      auto bwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate(B)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->rightColumns_);
      auto bwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse(B)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->leftColumns_);
      bwBFCreate->addBloomFilterUsePOp(bwBFUse);
      bwBFUse->consume(bwBFCreate);

      // add ops
      PrePToPTransformerUtil::addPhysicalOps({bwBFCreate, bwBFUse}, physicalOps_);

      // make predicate transfer graph nodes
      auto bwPTGraphNode = std::make_shared<PredTransGraphNode>(bwBFCreate, bwBFUse, rightPTUnit, leftPTUnit);
      bwPTGraphNodes_.emplace(bwPTGraphNode);
      rightPTUnit->bwOutPTNodes_.emplace_back(bwPTGraphNode);
      ++leftPTUnit->numBwBFUseToVisit_;
    }
  }
}

void PrePToPTransformerForPredTrans::connectFwBloomFilterOps() {
  // collect nodes with no bfUse to visit
  std::queue<std::shared_ptr<PredTransGraphNode>> freeNodes;
  for (auto nodeIt = fwPTGraphNodes_.begin(); nodeIt != fwPTGraphNodes_.end(); ) {
    if ((*nodeIt)->bfCreatePTUnit_.lock()->numFwBFUseToVisit_ == 0) {
      freeNodes.push(*nodeIt);
      nodeIt = fwPTGraphNodes_.erase(nodeIt);
    } else {
      ++nodeIt;
    }
  }

  // topological ordering
  while (!freeNodes.empty()) {
    const auto &node = freeNodes.front();
    freeNodes.pop();

    // connect for this node
    auto bfCreatePTUnit = node->bfCreatePTUnit_.lock();
    auto bfUsePTUnit = node->bfUsePTUnit_.lock();
    std::shared_ptr<PhysicalOp> bfCreate = node->bfCreate_;
    std::shared_ptr<PhysicalOp> bfUse = node->bfUse_;
    PrePToPTransformerUtil::connectOneToOne(bfCreatePTUnit->currUpConnOp_, bfCreate);
    PrePToPTransformerUtil::connectOneToOne(bfUsePTUnit->currUpConnOp_, bfUse);
    bfUsePTUnit->currUpConnOp_ = bfUse;
    ++bfUsePTUnit->numFwBFUseVisited_;

    // update the outgoing neighbors of this node
    if (bfUsePTUnit->numFwBFUseVisited_ == bfUsePTUnit->numFwBFUseToVisit_) {
      for (const auto &outNode: bfUsePTUnit->fwOutPTNodes_) {
        freeNodes.push(outNode);
        fwPTGraphNodes_.erase(outNode);
      }
    }
  }

  // throw exception when there is a cycle
  if (!fwPTGraphNodes_.empty()) {
    throw std::runtime_error("The join origins (base table joins) contain cycles (forward predicate transfer)");
  }
}

void PrePToPTransformerForPredTrans::connectBwBloomFilterOps() {
  // collect nodes with no bfUse to visit
  std::queue<std::shared_ptr<PredTransGraphNode>> freeNodes;
  for (auto nodeIt = bwPTGraphNodes_.begin(); nodeIt != bwPTGraphNodes_.end(); ) {
    if ((*nodeIt)->bfCreatePTUnit_.lock()->numBwBFUseToVisit_ == 0) {
      freeNodes.push(*nodeIt);
      nodeIt = bwPTGraphNodes_.erase(nodeIt);
    } else {
      ++nodeIt;
    }
  }

  // topological ordering
  while (!freeNodes.empty()) {
    const auto &node = freeNodes.front();
    freeNodes.pop();

    // connect for this node
    auto bfCreatePTUnit = node->bfCreatePTUnit_.lock();
    auto bfUsePTUnit = node->bfUsePTUnit_.lock();
    std::shared_ptr<PhysicalOp> bfCreate = node->bfCreate_;
    std::shared_ptr<PhysicalOp> bfUse = node->bfUse_;
    PrePToPTransformerUtil::connectOneToOne(bfCreatePTUnit->currUpConnOp_, bfCreate);
    PrePToPTransformerUtil::connectOneToOne(bfUsePTUnit->currUpConnOp_, bfUse);
    bfUsePTUnit->currUpConnOp_ = bfUse;
    ++bfUsePTUnit->numBwBFUseVisited_;

    // update the outgoing neighbors of this node
    if (bfUsePTUnit->numBwBFUseVisited_ == bfUsePTUnit->numBwBFUseToVisit_) {
      for (const auto &outNode: bfUsePTUnit->bwOutPTNodes_) {
        freeNodes.push(outNode);
        bwPTGraphNodes_.erase(outNode);
      }
    }
  }

  // throw exception when there is a cycle
  if (!bwPTGraphNodes_.empty()) {
    throw std::runtime_error("The join origins (base table joins) contain cycles (backward predicate transfer)");
  }
}

std::vector<std::shared_ptr<PhysicalOp>> PrePToPTransformerForPredTrans::transformExec() {
  // Update transformation results
  updateTransRes();

  // transform from root in dfs
  return transformDfs(prePhysicalPlan_->getRootOp());
}

void PrePToPTransformerForPredTrans::updateTransRes() {
  // update the transform result of FilterableScanPrePOp, if it participates in predicate transfer
  for (auto &transResIt: prePOpToTransRes_) {
    bool needToUpdate = true;
    std::vector<std::shared_ptr<PhysicalOp>> updatedTransRes;
    for (const auto &origConnOp: transResIt.second) {
      const auto &origUpConnOpToPTUnitIt = origUpConnOpToPTUnit_.find(origConnOp->name());
      if (origUpConnOpToPTUnitIt != origUpConnOpToPTUnit_.end()) {
        updatedTransRes.emplace_back(origUpConnOpToPTUnitIt->second->currUpConnOp_);
      } else {
        // no participation if predicate transfer
        needToUpdate = false;
        break;
      }
    }
    if (needToUpdate) {
      transResIt.second = updatedTransRes;
    }
  }
}

}
