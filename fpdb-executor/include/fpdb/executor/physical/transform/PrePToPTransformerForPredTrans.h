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
  static std::shared_ptr<PhysicalPlan> transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                                 const shared_ptr<CatalogueEntry> &catalogueEntry,
                                                 const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                                 const shared_ptr<Mode> &mode,
                                                 int parallelDegree,
                                                 int numNodes);

private:
  struct PredTransUnit;
  struct PredTransGraphNode;

  // basic unit for predicate transfer, i.e. ops (scan/local filter, BF create/use) corresponding to a single scan op
  // a unit can be viewed as a vertical chain from scan/local filter to subsequent BF use ops.
  struct PredTransUnit {
    std::shared_ptr<PhysicalOp> origUpConnOp_;    // the start of the vertical chain, also as the identifier
    std::shared_ptr<PhysicalOp> currUpConnOp_;    // the end of the vertical chain
    std::vector<std::shared_ptr<PredTransGraphNode>> fwOutPTNodes_, bwOutPTNodes_;  // in/out PT graph nodes
    int numFwBFUseToVisit_ = 0, numBwBFUseToVisit_ = 0;     // for dependencies
    int numFwBFUseVisited_ = 0, numBwBFUseVisited_ = 0;     // for dependencies

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
    std::weak_ptr<PredTransUnit> bfCreatePTUnit_, bfUsePTUnit_;   // the corresponding PT unit of BF create/use

    PredTransGraphNode(const std::shared_ptr<bloomfilter::BloomFilterCreatePOp> &bfCreate,
                       const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bfUse,
                       const std::shared_ptr<PredTransUnit> &bfCreatePTUnit,
                       const std::shared_ptr<PredTransUnit> &bfUsePTUnit):
      bfCreate_(bfCreate), bfUse_(bfUse), bfCreatePTUnit_(bfCreatePTUnit), bfUsePTUnit_(bfUsePTUnit) {}

    size_t hash() const {
      return std::hash<std::string>()(bfCreate_->name() + " - " + bfUse_->name());
    }

    bool equalTo(const std::shared_ptr<PredTransGraphNode> &other) const {
      return bfCreate_->name() == other->bfCreate_->name() && bfUse_->name() == other->bfUse_->name();
    }
  };

  struct PredTransGraphNodePtrHash {
    inline size_t operator()(const std::shared_ptr<PredTransGraphNode> &node) const {
      return node->hash();
    }
  };

  struct PredTransGraphNodePtrPred {
    inline bool operator()(const std::shared_ptr<PredTransGraphNode> &lhs,
                           const std::shared_ptr<PredTransGraphNode> &rhs) const {
      return lhs->equalTo(rhs);
    }
  };

  PrePToPTransformerForPredTrans(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                 const shared_ptr<CatalogueEntry> &catalogueEntry,
                                 const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                 const shared_ptr<Mode> &mode,
                                 int parallelDegree,
                                 int numNodes);

  /**
   * Main entry for transformation
   */
  std::shared_ptr<PhysicalPlan> transform() override;

  /**
   * Phase1: generate a partial plan for predicate transfer using bloom filters
   */
  void transformPredTrans();

  // currently pushdown is not supported
  vector<shared_ptr<PhysicalOp>> transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &prePOp) override;

  // Construct bloom filter ops from pairs of base table joins
  void makeBloomFilterOps(const std::unordered_set<std::shared_ptr<JoinOrigin>,
          JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  // Connect bloom filter ops created above, in a topological order
  void connectFwBloomFilterOps();
  void connectBwBloomFilterOps();

  /**
   * Phase 2: generate a partial plan for execution after predicate transfer, and connect to the partial plan of Phase 1
   * @return physical ops to be connected to the final collate op
   */
  std::vector<std::shared_ptr<PhysicalOp>> transformExec();

  // state maintained during transformation
  std::unordered_map<int, std::vector<std::shared_ptr<PhysicalOp>>> filterableScanTransRes_;
  std::unordered_set<std::shared_ptr<PredTransUnit>, PredTransUnitPtrHash, PredTransUnitPtrPred> ptUnits_;
  std::unordered_set<std::shared_ptr<PredTransGraphNode>, PredTransGraphNodePtrHash, PredTransGraphNodePtrPred>
      fwPTGraphNodes_, bwPTGraphNodes_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H
