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
                    const std::vector<std::string> &projectColumnNames);

  void onReceive(const Envelope &msg) override;

  void setLeftProducer(const std::shared_ptr<PhysicalOp> &leftProducer);
  void setRightProducer(const std::shared_ptr<PhysicalOp> &rightProducer);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void merge();

  std::weak_ptr<PhysicalOp> leftProducer_;
  std::weak_ptr<PhysicalOp> rightProducer_;

  std::list<std::shared_ptr<TupleSet>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet>> rightTupleSets_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H
