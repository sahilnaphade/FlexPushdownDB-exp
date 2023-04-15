//
// Created by Yifei Yang on 4/11/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>

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

  /**
   * Return nullopt if predicate transfer is blocked
   */
  std::optional<std::vector<std::shared_ptr<PhysicalOp>>>
  transformDfsPredTrans(const std::shared_ptr<PrePhysicalOp> &prePOp);
  std::optional<std::vector<std::vector<std::shared_ptr<PhysicalOp>>>>
  transformProducersPredTrans(const shared_ptr<PrePhysicalOp> &prePOp);
  std::optional<std::vector<std::shared_ptr<PhysicalOp>>>
  transformHashJoinPredTrans(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  /**
   * Transform prePhysicalPlan to a physical plan for execution after predicate transfer
   */
  std::shared_ptr<PhysicalPlan> transformExec();

  // state maintained during transformation

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERFORPREDTRANS_H
