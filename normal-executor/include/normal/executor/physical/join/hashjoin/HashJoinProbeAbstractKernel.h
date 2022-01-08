//
// Created by Yifei Yang on 12/15/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H

#include <normal/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/TupleSetIndex.h>
#include <set>
#include <memory>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::join {

/**
 * Abstract class for kernel of hash join, derived class includes HashJoinProbeKernel, HashSemiJoinProbeKernel
 */
class HashJoinProbeAbstractKernel {
  
public:
  HashJoinProbeAbstractKernel(HashJoinPredicate pred, set<string> neededColumnNames);
  virtual ~HashJoinProbeAbstractKernel() = default;

  virtual tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) = 0;
  virtual tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) = 0;
  virtual tl::expected<void, string> finalize() = 0;

  const optional<shared_ptr<normal::tuple::TupleSet>> &getBuffer() const;
  const optional<shared_ptr<::arrow::Schema>> &getOutputSchema() const;

  void clearBuffer();

protected:
  tl::expected<void, string> putBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex);
  tl::expected<void, string> putProbeTupleSet(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void bufferOutputSchema(const shared_ptr<TupleSetIndex> &tupleSetIndex, const shared_ptr<TupleSet> &tupleSet);

  HashJoinPredicate pred_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in build/probe table
  optional<shared_ptr<::arrow::Schema>> outputSchema_;
  optional<shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
  optional<shared_ptr<TupleSet>> probeTupleSet_;

private:
  tl::expected<void, string> validateColumnNames(const shared_ptr<arrow::Schema> &schema,
                                                 const vector<string> &columnNames);

  set<string> neededColumnNames_;
  optional<shared_ptr<normal::tuple::TupleSet>> buffer_;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H
