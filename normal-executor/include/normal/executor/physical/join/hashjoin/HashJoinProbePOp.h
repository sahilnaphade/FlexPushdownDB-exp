//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <normal/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/plan/prephysical/JoinType.h>

using namespace normal::executor::message;
using namespace normal::plan::prephysical;
using namespace std;

namespace normal::executor::physical::join {

/**
 * Operator implementing probe phase of a hash join
 *
 * Takes hashtable produced from build phase on one of the relations in the join (ideally the smaller) and uses it
 * to select rows from the both relations to include in the join.
 *
 */
class HashJoinProbePOp : public PhysicalOp {

public:
  HashJoinProbePOp(string name,
                   HashJoinPredicate pred,
                   JoinType joinType,
                   vector<string> projectColumnNames);

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onHashTable(const TupleSetIndexMessage &msg);
  void onComplete(const CompleteMessage &msg);

  void send(bool force);

  shared_ptr<HashJoinProbeAbstractKernel> kernel_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H