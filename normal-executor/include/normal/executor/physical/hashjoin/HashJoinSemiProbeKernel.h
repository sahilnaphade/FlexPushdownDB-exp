//
// Created by Yifei Yang on 12/15/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINSEMIPROBEKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINSEMIPROBEKERNEL_H

#include <normal/executor/physical/hashjoin/HashJoinProbeAbstractKernel.h>

namespace normal::executor::physical::hashjoin {

class HashJoinSemiProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashJoinSemiProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames);
  static shared_ptr<HashJoinSemiProbeKernel> make(HashJoinPredicate pred, set<string> neededColumnNames);

  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINSEMIPROBEKERNEL_H
