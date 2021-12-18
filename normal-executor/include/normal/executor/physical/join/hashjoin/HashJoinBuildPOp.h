//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H

#include <normal/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <arrow/scalar.h>
#include <unordered_map>

using namespace normal::executor::message;
using namespace std;

namespace normal::executor::physical::join {

/**
 * Operator implementing build phase of a hash join
 *
 * Builds a hash table of tuples from one of the relations in a join (ideally the smaller relation). That hashtable is
 * then used in the probe phase to select rows to add to the final joined relation.
 *
 */
class HashJoinBuildPOp : public PhysicalOp {

public:
  explicit HashJoinBuildPOp(const string &name,
                            const vector<string> &columnNames,
                            const vector<string> &projectColumnNames);

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void send(bool force);

  HashJoinBuildKernel kernel_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
