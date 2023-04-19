//
// Created by Yifei Yang on 4/11/23.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerForPredTrans.h>
#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::executor::physical {

PrePToPTransformerForPredTrans::PrePToPTransformerForPredTrans(
        const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
        const shared_ptr<CatalogueEntry> &catalogueEntry,
        const shared_ptr<ObjStoreConnector> &objStoreConnector,
        const shared_ptr<Mode> &mode,
        int parallelDegree,
        int numNodes):
  PrePToPTransformer(prePhysicalPlan, catalogueEntry, objStoreConnector, mode, parallelDegree, numNodes) {}

std::pair<std::shared_ptr<PhysicalPlan>, std::shared_ptr<PhysicalPlan>>
PrePToPTransformerForPredTrans::transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                          const shared_ptr<CatalogueEntry> &catalogueEntry,
                                          const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                          const shared_ptr<Mode> &mode,
                                          int parallelDegree,
                                          int numNodes) {
  PrePToPTransformerForPredTrans transformer(prePhysicalPlan, catalogueEntry, objStoreConnector,
                                             mode, parallelDegree, numNodes);
  return {transformer.transformPredTrans(), transformer.transformExec()};
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transformPredTrans() {
  // extract base table joins
  auto joinOrigins = JoinOriginTracer::trace(prePhysicalPlan_);
  makeBloomFilterOps(joinOrigins);
  return nullptr;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformerForPredTrans::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &prePOp) {
  // check if this prepOp has already been visited, since one may be visited multiple times
  auto transformResIt = filterableScanTransRes_.find(prePOp->getId());
  if (transformResIt != filterableScanTransRes_.end()) {
    return transformResIt->second;
  }

  if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
    throw runtime_error(fmt::format("Unsupported catalogue entry type for filterable scan prephysical operator during "
                                    "predicate transfer: {}", catalogueEntry_->getTypeName()));
  }
  auto objStoreCatalogueEntry = std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_);
  if (objStoreCatalogueEntry->getStoreType() == ObjStoreType::S3) {
    throw runtime_error(fmt::format("Unsupported object store type for filterable scan prephysical operator during "
                                    "predicate transfer: {}", objStoreCatalogueEntry->getStoreTypeName()));
  }

  // transfer filterable scan in pullup mode
  auto fpdbStoreConnector = static_pointer_cast<obj_store::FPDBStoreConnector>(objStoreConnector_);
  auto separableSuperPrePOp = std::make_shared<SeparableSuperPrePOp>(prePOp->getId(), prePOp);
  auto transformRes = PrePToFPDBStorePTransformer::transform(separableSuperPrePOp, Mode::pullupMode(), numNodes_,
                                                             parallelDegree_, parallelDegree_, fpdbStoreConnector);
  PrePToPTransformerUtil::addPhysicalOps(transformRes.second, physicalOps_);
  filterableScanTransRes_[prePOp->getId()] = transformRes.first;
  return transformRes.first;
}

void PrePToPTransformerForPredTrans::makeBloomFilterOps(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  for (const auto &joinOrigin: joinOrigins) {
    // transform the base table scan (+filter) ops
    auto upLeftConnPOps = transformFilterableScan(joinOrigin->left_);
    auto upRightConnPOps = transformFilterableScan(joinOrigin->right_);
    // FIXME: currently only support single-partition tables
    if (upLeftConnPOps.size() != 1 || upRightConnPOps.size() != 1) {
      throw runtime_error("Currently only support single-partition tables");
    }
    auto upLeftConnPOp = upLeftConnPOps[0];
    auto upRightConnPOp = upRightConnPOps[0];

    // FIXME: currently only support inner and semi joins, but in fact can transfer in a single direction for left/right joins
    if (joinOrigin->joinType_ != JoinType::INNER && joinOrigin->joinType_ != JoinType::LEFT_SEMI
        && joinOrigin->joinType_ != JoinType::RIGHT_SEMI) {
      continue;
    }
    join::HashJoinPredicate hashJoinPredicate(joinOrigin->leftColumns_, joinOrigin->rightColumns_);
    const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

    // forward bloom filter
    auto fwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
            fmt::format("BloomFilterCreate(F)-{}", hashJoinPredicateStr),
            std::vector<std::string>{},
            0,
            joinOrigin->leftColumns_);
    auto fwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
            fmt::format("BloomFilterUse(F)-{}", hashJoinPredicateStr),
            std::vector<std::string>{},
            0,
            joinOrigin->rightColumns_);
    fwBFCreate->addBloomFilterUsePOp(fwBFUse);
    fwBFUse->consume(fwBFCreate);

    // backward bloom filter
    auto bwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
            fmt::format("BloomFilterCreate(B)-{}", hashJoinPredicateStr),
            std::vector<std::string>{},
            0,
            joinOrigin->rightColumns_);
    auto bwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
            fmt::format("BloomFilterUse(B)-{}", hashJoinPredicateStr),
            std::vector<std::string>{},
            0,
            joinOrigin->leftColumns_);
    bwBFCreate->addBloomFilterUsePOp(bwBFUse);
    bwBFUse->consume(bwBFCreate);

    // add ops
    PrePToPTransformerUtil::addPhysicalOps({fwBFCreate, fwBFUse, bwBFCreate, bwBFUse}, physicalOps_);
    // make predicate transfer units
    auto leftPTUnit = std::make_shared<PredTransUnit>(upLeftConnPOp);
    auto ptUnitIt = ptUnits_.find(leftPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(leftPTUnit);
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(upRightConnPOp);
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ++rightPTUnit->numBFUseToVisit_;
      ptUnits_.emplace(rightPTUnit);
    } else {
      rightPTUnit = *ptUnitIt;
      ++rightPTUnit->numBFUseToVisit_;
    }
    // make predicate transfer graph nodes
    auto fwPTGraphNode = std::make_shared<PredTransGraphNode>(fwBFCreate, fwBFUse, leftPTUnit, rightPTUnit);
    auto bwPTGraphNode = std::make_shared<PredTransGraphNode>(bwBFCreate, bwBFUse, rightPTUnit, leftPTUnit);
    fwPTGraphNodes_.emplace_back(fwPTGraphNode);
    bwPTGraphNodes_.emplace_back(bwPTGraphNode);
    leftPTUnit->outPTNodes_.emplace_back(fwPTGraphNode);
  }
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transformExec() {
  return nullptr;
}

}
