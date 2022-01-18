//
// Created by matt on 20/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/tuple/TupleSet.h>
#include <queue>

using namespace normal::executor::message;

namespace normal::executor::physical::merge {

class MergePOp : public PhysicalOp {

public:
  explicit MergePOp(const std::string &name,
                    const std::vector<std::string> &projectColumnNames,
                    int nodeId);
  MergePOp() = default;
  MergePOp(const MergePOp&) = default;
  MergePOp& operator=(const MergePOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;

  void setLeftProducer(const std::shared_ptr<PhysicalOp> &leftProducer);
  void setRightProducer(const std::shared_ptr<PhysicalOp> &rightProducer);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void merge();

  std::string leftProducerName_;
  std::string rightProducerName_;

  std::list<std::shared_ptr<TupleSet>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet>> rightTupleSets_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, MergePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("leftProducerName", op.leftProducerName_),
                               f.field("rightProducerName", op.rightProducerName_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H
