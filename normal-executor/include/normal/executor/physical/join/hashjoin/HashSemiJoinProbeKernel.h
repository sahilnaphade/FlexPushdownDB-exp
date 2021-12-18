//
// Created by Yifei Yang on 12/15/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H

#include <normal/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <normal/tuple/TupleSet.h>
#include <normal/expression/gandiva/Filter.h>
#include <unordered_set>

namespace normal::executor::physical::join {

class HashSemiJoinProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashSemiJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames);
  static shared_ptr<HashSemiJoinProbeKernel> make(HashJoinPredicate pred, set<string> neededColumnNames);

  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;
  tl::expected<void, string> finalize() override;

private:
  static tl::expected<shared_ptr<::gandiva::SelectionVector>, string>
  makeSelectionVector(const unordered_set<int64_t> &rowMatchIndexes);

  unordered_set<int64_t> rowMatchIndexes_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H
