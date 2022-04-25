//
// Created by Yifei Yang on 4/25/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWKERNEL_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/plan/prephysical/JoinType.h>
#include <fpdb/tuple/TupleSet.h>
#include <arrow/compute/exec/options.h>
#include <arrow/util/async_generator.h>
#include <set>

using namespace fpdb::plan::prephysical;
using namespace fpdb::tuple;

namespace fpdb::executor::physical::join {

class HashJoinArrowKernel {

public:
  HashJoinArrowKernel(const HashJoinPredicate &pred,
                      const std::set<std::string> &neededColumnNames,
                      JoinType joinType);

  static std::shared_ptr<HashJoinArrowKernel> make(const HashJoinPredicate &pred,
                                                   const std::set<std::string> &neededColumnNames,
                                                   JoinType joinType);

  tl::expected<std::shared_ptr<TupleSet>, std::string> join(const std::shared_ptr<TupleSet> &leftTupleSet,
                                                            const std::shared_ptr<TupleSet> &rightTupleSet);

  tl::expected<void, std::string> joinLeftTupleSet(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, std::string> joinRightTupleSet(const std::shared_ptr<TupleSet> &tupleSet);

private:
  HashJoinPredicate pred_;
  std::set<std::string> neededColumnNames_;
  JoinType joinType_;

  std::optional<std::shared_ptr<TupleSet>> leftTupleSet_;
  std::optional<std::shared_ptr<TupleSet>> rightTupleSet_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWKERNEL_H
