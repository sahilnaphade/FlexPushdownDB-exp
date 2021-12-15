//
// Created by Yifei Yang on 12/15/21.
//

#include <normal/executor/physical/hashjoin/HashJoinSemiProbeKernel.h>
#include <arrow/api.h>
#include <utility>

namespace normal::executor::physical::hashjoin {

HashJoinSemiProbeKernel::HashJoinSemiProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames)) {}

shared_ptr<HashJoinSemiProbeKernel> HashJoinSemiProbeKernel::make(HashJoinPredicate pred, set<string> neededColumnNames) {
  return make_shared<HashJoinSemiProbeKernel>(move(pred), move(neededColumnNames));
}

tl::expected<void, string> HashJoinSemiProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex) {
  return {};
}

tl::expected<void, string> HashJoinSemiProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet) {
  return {};
}

}
