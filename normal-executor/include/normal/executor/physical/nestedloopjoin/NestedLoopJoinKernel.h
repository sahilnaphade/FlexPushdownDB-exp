//
// Created by Yifei Yang on 12/12/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H

#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <set>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::nestedloopjoin {

class NestedLoopJoinKernel {

public:
  NestedLoopJoinKernel(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                       const set<string> &neededColumnNames);

  static NestedLoopJoinKernel make(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                   const set<string> &neededColumnNames);

  tl::expected<void, string> joinIncomingLeft(const shared_ptr<TupleSet> &incomingLeft);
  tl::expected<void, string> joinIncomingRight(const shared_ptr<TupleSet> &incomingRight);

  const optional<shared_ptr<TupleSet>> &getBuffer() const;
  void clear();

private:
  tl::expected<shared_ptr<TupleSet>, string> join(const shared_ptr<TupleSet> &leftTupleSet,
                                                  const shared_ptr<TupleSet> &rightTupleSet);
  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void bufferOutputSchema(const shared_ptr<TupleSet> &leftTupleSet, const shared_ptr<TupleSet> &rightTupleSet);
  
  optional<shared_ptr<expression::gandiva::Expression>> predicate_;
  optional<shared_ptr<TupleSet>> leftTupleSet_;
  optional<shared_ptr<TupleSet>> rightTupleSet_;
  set<string> neededColumnNames_;
  optional<shared_ptr<::arrow::Schema>> outputSchema_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in left/right table
  optional<shared_ptr<TupleSet>> buffer_;
  
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H
