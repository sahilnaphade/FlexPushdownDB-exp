//
// Created by Yifei Yang on 12/12/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/join/nestedloopjoin/NestedLoopJoinKernel.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/plan/prephysical/JoinType.h>

using namespace normal::executor::message;
using namespace normal::plan::prephysical;
using namespace normal::expression::gandiva;
using namespace std;

namespace normal::executor::physical::join {

class NestedLoopJoinPOp : public PhysicalOp {
public:
  NestedLoopJoinPOp(const string &name,
                    const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                    JoinType joinType,
                    const vector<string> &projectColumnNames);

  void onReceive(const Envelope &msg) override;

  void addLeftProducer(const shared_ptr<PhysicalOp> &leftProducer);
  void addRightProducer(const shared_ptr<PhysicalOp> &rightProducer);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);
  void send(bool force);

  set<string> leftProducerNames_;
  set<string> rightProducerName_;

  shared_ptr<NestedLoopJoinKernel> kernel_;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H
