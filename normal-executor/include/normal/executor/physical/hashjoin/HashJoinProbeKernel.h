//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H

#include <normal/executor/physical/hashjoin/HashJoinProbeAbstractKernel.h>

namespace normal::executor::physical::hashjoin {

class HashJoinProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames);
  static shared_ptr<HashJoinProbeKernel> make(HashJoinPredicate pred, set<string> neededColumnNames);

  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
