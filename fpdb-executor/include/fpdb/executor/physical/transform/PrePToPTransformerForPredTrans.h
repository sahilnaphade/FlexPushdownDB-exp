//
// Created by Yifei Yang on 4/11/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/plan/prephysical/JoinOriginTracer.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical {

/**
 * Used specifically for predicate transfer
 */
class PrePToPTransformerForPredTrans: public PrePToPTransformer {

public:
  /**
   * Return two physical plans, one for predicate transfer, another for execution
   */
  static std::pair<std::shared_ptr<PhysicalPlan>, std::shared_ptr<PhysicalPlan>>
  transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
            const shared_ptr<CatalogueEntry> &catalogueEntry,
            const shared_ptr<ObjStoreConnector> &objStoreConnector,
            const shared_ptr<Mode> &mode,
            int parallelDegree,
            int numNodes);

private:
  struct PredTransUnit;
  struct PredTransGraphNode;

  // basic unit for predicate transfer, i.e. ops (scan BF create/use) corresponding to a single scan op
  struct PredTransUnit {
    std::shared_ptr<PhysicalOp> origUpConnOp_;
    std::shared_ptr<PhysicalOp> currUpConnOp_;
    std::vector<std::weak_ptr<PredTransGraphNode>> outPTNodes_;
    int numBFUseToVisit_ = 0;
    int numBFUseVisited_ = 0;

    PredTransUnit(const std::shared_ptr<PhysicalOp> &upConnOp):
      origUpConnOp_(upConnOp), currUpConnOp_(upConnOp) {}

    size_t hash() const {
      return std::hash<std::string>()(origUpConnOp_->name());
    }

    bool equalTo(const std::shared_ptr<PredTransUnit> &other) const {
      return origUpConnOp_->name() == other->origUpConnOp_->name();
    }
  };

  struct PredTransUnitPtrHash {
    inline size_t operator()(const std::shared_ptr<PredTransUnit> &joinOrigin) const {
      return joinOrigin->hash();
    }
  };

  struct PredTransUnitPtrPred {
    inline bool operator()(const std::shared_ptr<PredTransUnit> &lhs, const std::shared_ptr<PredTransUnit> &rhs) const {
      return lhs->equalTo(rhs);
    }
  };

  // basic node in predicate transfer dependency graph, i.e. a pair of BF create/use
  // the pairs of BF create/use will be ordered and connected based on the dependency graph
  struct PredTransGraphNode {
    std::shared_ptr<bloomfilter::BloomFilterCreatePOp> bfCreate_;
    std::shared_ptr<bloomfilter::BloomFilterUsePOp> bfUse_;
    std::shared_ptr<PredTransUnit> bfCreatePTUnit_, bfUsePTUnit_;

    PredTransGraphNode(const std::shared_ptr<bloomfilter::BloomFilterCreatePOp> &bfCreate,
                       const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bfUse,
                       const std::shared_ptr<PredTransUnit> &bfCreatePTUnit,
                       const std::shared_ptr<PredTransUnit> &bfUsePTUnit):
      bfCreate_(bfCreate), bfUse_(bfUse), bfCreatePTUnit_(bfCreatePTUnit), bfUsePTUnit_(bfUsePTUnit) {}
  };

  PrePToPTransformerForPredTrans(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                 const shared_ptr<CatalogueEntry> &catalogueEntry,
                                 const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                 const shared_ptr<Mode> &mode,
                                 int parallelDegree,
                                 int numNodes);
  /**
   * Transform prePhysicalPlan to a physical plan for predicate transfer
   */
  std::shared_ptr<PhysicalPlan> transformPredTrans();

  vector<shared_ptr<PhysicalOp>> transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &prePOp) override;

  /**
   * Construct bloom filter ops from pairs of base table joins
   */
  void makeBloomFilterOps(const std::unordered_set<std::shared_ptr<JoinOrigin>,
          JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  /**
   * Transform prePhysicalPlan to a physical plan for execution after predicate transfer
   */
  std::shared_ptr<PhysicalPlan> transformExec();

  // state maintained during transformation
  std::unordered_map<int, std::vector<std::shared_ptr<PhysicalOp>>> filterableScanTransRes_;
  std::unordered_set<std::shared_ptr<PredTransUnit>, PredTransUnitPtrHash, PredTransUnitPtrPred> ptUnits_;
  std::vector<std::shared_ptr<PredTransGraphNode>> fwPTGraphNodes_, bwPTGraphNodes_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H
