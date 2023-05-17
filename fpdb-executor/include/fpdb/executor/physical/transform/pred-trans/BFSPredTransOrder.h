//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <stack>

namespace fpdb::executor::physical {

class BFSPredTransOrder: public PredTransOrder {

public:
  BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer);
  ~BFSPredTransOrder() override = default;

private:
  enum TransferDir {
    FORWARD_ONLY,
    BACKWARD_ONLY,
    BOTH
  };

  struct PredTransUnit {
    uint prePOpId_;       // the prephysical op id of the corresponding FilterableScanPrePOp
    std::shared_ptr<PhysicalOp> origUpConnOp_;    // the start of the vertical chain, also as the identifier
    std::shared_ptr<PhysicalOp> currUpConnOp_;    // the end of the vertical chain
    std::vector<std::pair<std::weak_ptr<PredTransUnit>, TransferDir>> neighbors_;     // neighbors in the join graph

    PredTransUnit(uint prePOpId, const std::shared_ptr<PhysicalOp> &upConnOp):
      prePOpId_(prePOpId),
      origUpConnOp_(upConnOp), currUpConnOp_(upConnOp) {}

    size_t hash() const {
      return std::hash<std::string>()(origUpConnOp_->name());
    }

    bool equalTo(const std::shared_ptr<PredTransUnit> &other) const {
      return origUpConnOp_->name() == other->origUpConnOp_->name();
    }
  };

  struct PredTransUnitPtrHash {
    inline size_t operator()(const std::shared_ptr<PredTransUnit> &ptUnit) const {
      return ptUnit->hash();
    }
  };

  struct PredTransUnitPtrPred {
    inline bool operator()(const std::shared_ptr<PredTransUnit> &lhs, const std::shared_ptr<PredTransUnit> &rhs) const {
      return lhs->equalTo(rhs);
    }
  };

  struct BFSPredTransPair {
    std::shared_ptr<PredTransUnit> leftPTUnit_;
    std::shared_ptr<PredTransUnit> rightPTUnit_;
    TransferDir dir_;

    BFSPredTransPair(const std::shared_ptr<PredTransUnit> &leftPTUnit,
                     const std::shared_ptr<PredTransUnit> &rightPTUnit,
                     TransferDir dir):
      leftPTUnit_(leftPTUnit), rightPTUnit_(rightPTUnit), dir_(dir) {}
  };

  void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) override;

  void makePTUnits(
          const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);
  void bfsSearch();

  /**
   * extra states maintained during transformation
   */
  // pred-trans units
  std::unordered_set<std::shared_ptr<PredTransUnit>, PredTransUnitPtrHash, PredTransUnitPtrPred> ptUnits_;
  std::shared_ptr<PredTransUnit> rootPTUnit_;
  double rootPTUnitRowCount_;

  // BFS ordering result, the order is the reverse of BFS search so use a stack here
  std::stack<std::shared_ptr<BFSPredTransPair>> bfsPredTransOrder_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
