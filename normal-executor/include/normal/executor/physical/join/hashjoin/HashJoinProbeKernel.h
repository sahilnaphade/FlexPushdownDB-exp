//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H

#include <normal/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <normal/executor/physical/join/hashjoin/RecordBatchHashJoiner.h>
#include <normal/executor/physical/join/OuterJoinHelper.h>

namespace normal::executor::physical::join {

class HashJoinProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashJoinProbeKernel(HashJoinPredicate pred,
                      set<string> neededColumnNames,
                      bool isLeft,
                      bool isRight);
  static shared_ptr<HashJoinProbeKernel> make(HashJoinPredicate pred,
                                              set<string> neededColumnNames,
                                              bool isLeft,
                                              bool isRight);

  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;
  tl::expected<void, string> finalize() override;

private:
  tl::expected<shared_ptr<normal::tuple::TupleSet>, string> join(const shared_ptr<RecordBatchHashJoiner> &joiner,
                                                                 const shared_ptr<TupleSet> &probeTupleSet,
                                                                 int64_t probeRowOffset);
  tl::expected<void, string> makeOuterJoinHelpers();
  tl::expected<void, string> computeOuterJoin();

  bool isLeft_;
  bool isRight_;
  bool isOuterJoinHelperCreated_ = false;
  optional<shared_ptr<OuterJoinHelper>> leftJoinHelper_;
  optional<shared_ptr<OuterJoinHelper>> rightJoinHelper_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
