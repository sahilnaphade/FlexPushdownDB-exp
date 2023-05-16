//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PrePToPTransformerForPredTrans.h>
#include <fpdb/plan/prephysical/JoinOriginTracer.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical {

enum PredTransOrderType {
  SMALL_TO_LARGE,
  BFS,
  UNKNOWN
};

class PredTransOrder {

public:
  static void orderPredTrans(
          PredTransOrderType type,
          PrePToPTransformerForPredTrans* transformer,
          const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  PredTransOrder(PredTransOrderType type,
                 PrePToPTransformerForPredTrans* transformer);
  virtual ~PredTransOrder() = default;

  PredTransOrderType getType() const;

private:
  /**
   * Make the order of predicate transfer
   * Updated parameters: physicalOps, prePOpToTransRes
   */
  virtual void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) = 0;

  PredTransOrderType type_;

protected:
  PrePToPTransformerForPredTrans* transformer_;     // the transformer that calls to order predicate transfer

  /**
   * states maintained during transformation
   */
  // generate unique id for bloom filter ops for each join origin
  // note this is different from prePOpId used for other ops
  std::atomic<uint> bfIdGen_ = 0;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H
