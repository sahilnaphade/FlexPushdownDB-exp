//
// Created by Yifei Yang on 4/22/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_DUMMYNODE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_DUMMYNODE_H

#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>

namespace arrow::compute {

/**
 * A dummy node for arrow execution engine which just forwards received batches to its consumer
 */
class DummyNode: public ExecNode {

public:
  DummyNode(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema):
    ExecNode(plan, {}, {}, outputSchema, 1) {}

  static Result<ExecNode*> Make(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema) {
    return plan->EmplaceNode<DummyNode>(plan, outputSchema);
  }

  const char* kind_name() const override { return "DummyNode"; }

  void InputReceived(ExecNode*, ExecBatch batch) override {
    outputs_[0]->InputReceived(this, std::move(batch));
  }
  void ErrorReceived(ExecNode*, Status) override {}
  void InputFinished(ExecNode*, int total_batches) override {
    outputs_[0]->InputFinished(this, total_batches);
  }

  Status StartProducing() override {
    finished_ = Future<>::Make();
    return Status::OK();
  }

  void PauseProducing(ExecNode*) override {}

  void ResumeProducing(ExecNode*) override {}

  void StopProducing(ExecNode*) override {
    StopProducing();
  }

  void StopProducing() override {
    finished_.MarkFinished();
  }

  Future<> finished() override { return finished_; }

private:
  Future<> finished_ = Future<>::MakeFinished();

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_DUMMYNODE_H
