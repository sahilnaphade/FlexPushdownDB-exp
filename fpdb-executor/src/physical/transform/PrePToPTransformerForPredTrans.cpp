//
// Created by Yifei Yang on 4/11/23.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerForPredTrans.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/plan/prephysical/JoinOriginTracer.h>

using namespace fpdb::plan::prephysical;

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
  auto joinOrigins = JoinOriginTracer::trace(prePhysicalPlan);
  return {nullptr, nullptr};
//  auto predTransPlan = transformer.transformPredTrans();
//  transformer.clear();
//  auto execPlan = transformer.transformExec();
//  return {predTransPlan, execPlan};
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transformPredTrans() {
  // transform from root in dfs
  auto ops = transformDfsPredTrans(prePhysicalPlan_->getRootOp());

  // physical plan for predicate transfer does not need a root (collate) op
  return make_shared<PhysicalPlan>(physicalOps_, "");
}

std::optional<std::vector<std::shared_ptr<PhysicalOp>>>
PrePToPTransformerForPredTrans::transformDfsPredTrans(const std::shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    // currently skip ops that block predicate transfer
    case PrePOpType::SORT:
    case PrePOpType::LIMIT_SORT:
    case PrePOpType::AGGREGATE:
    case PrePOpType::GROUP:
    case PrePOpType::PROJECT:
    case PrePOpType::NESTED_LOOP_JOIN:
    // currently skip pushdown execution
    case PrePOpType::SEPARABLE_SUPER: {
      return std::nullopt;
    }
    case PrePOpType::FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    case PrePOpType::HASH_JOIN: {
      const auto &hashJoinPrePOp = std::static_pointer_cast<HashJoinPrePOp>(prePOp);
      return transformHashJoinPredTrans(hashJoinPrePOp);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

std::optional<std::vector<std::vector<std::shared_ptr<PhysicalOp>>>>
PrePToPTransformerForPredTrans::transformProducersPredTrans(const shared_ptr<PrePhysicalOp> &prePOp) {
  std::vector<std::vector<std::shared_ptr<PhysicalOp>>> transformResVec;
  for (const auto &producer: prePOp->getProducers()) {
    auto transformRes = transformDfsPredTrans(producer);
    if (!transformRes.has_value()) {
      return std::nullopt;
    }
    transformResVec.emplace_back(*transformRes);
  }
  return transformResVec;
}

std::optional<std::vector<std::shared_ptr<PhysicalOp>>>
PrePToPTransformerForPredTrans::transformHashJoinPredTrans(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // id
  auto prePOpId = hashJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducersPredTrans(hashJoinPrePOp);
  if (!producersTransRes.has_value()) {
    return std::nullopt;
  }
  if (producersTransRes->size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for hashJoin, should be {}, but get {}",
                                    2, producersTransRes->size()));
  }
  auto upLeftConnPOps = (*producersTransRes)[0];
  auto upRightConnPOps = (*producersTransRes)[1];
  // FIXME: currently only support single-partition tables
  if (upLeftConnPOps.size() != 1 || upRightConnPOps.size() != 1) {
    throw runtime_error("Currently only support single-partition tables");
  }
  auto upLeftConnPOp = upRightConnPOps[0];
  auto upRightConnPOp = upRightConnPOps[0];

  // join info
  auto joinType = hashJoinPrePOp->getJoinType();
  // FIXME: currently only support inner joins, but in fact can transfer in a single direction for left/right joins
  if (joinType != JoinType::INNER && joinType != JoinType::SEMI) {
    return std::nullopt;
  }
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();
  join::HashJoinPredicate hashJoinPredicate(leftColumnNames, rightColumnNames);
  const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

  // forward bloom filter
  std::shared_ptr<PhysicalOp> forwardBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
          fmt::format("BloomFilterCreate(F)[{}]-{}", prePOpId, hashJoinPredicateStr),
          std::vector<std::string>{},
          0,
          leftColumnNames);
  std::shared_ptr<PhysicalOp> forwardBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
          fmt::format("BloomFilterUse(F)[{}]-{}", prePOpId, hashJoinPredicateStr),
          std::vector<std::string>{},
          0,
          rightColumnNames);
  PrePToPTransformerUtil::connectOneToOne(upLeftConnPOp, forwardBFCreate);
  PrePToPTransformerUtil::connectOneToOne(upRightConnPOp, forwardBFUse);
  static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(forwardBFCreate)->addBloomFilterUsePOp(forwardBFUse);
  forwardBFUse->consume(forwardBFCreate);

  // backward bloom filter
  std::shared_ptr<PhysicalOp> backwardBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
          fmt::format("BloomFilterCreate(B)[{}]-{}", prePOpId, hashJoinPredicateStr),
          std::vector<std::string>{},
          0,
          rightColumnNames);
  std::shared_ptr<PhysicalOp> backwardBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
          fmt::format("BloomFilterUse(B)[{}]-{}", prePOpId, hashJoinPredicateStr),
          std::vector<std::string>{},
          0,
          leftColumnNames);
  static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(backwardBFCreate)->addBloomFilterUsePOp(backwardBFUse);
  backwardBFUse->consume(backwardBFCreate);

  return std::nullopt;
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transformExec() {
  return nullptr;
}

}
