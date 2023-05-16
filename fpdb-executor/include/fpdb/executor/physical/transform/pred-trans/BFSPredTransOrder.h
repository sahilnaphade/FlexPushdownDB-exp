//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>

namespace fpdb::executor::physical {

class BFSPredTransOrder: public PredTransOrder {

public:
  BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer);
  ~BFSPredTransOrder() override = default;

private:
  void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) override;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
