//
// Created by Yifei Yang on 12/12/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H

#include <normal/executor/physical/join/OuterJoinHelper.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <set>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::join {

class NestedLoopJoinKernel {

public:
  NestedLoopJoinKernel(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                       const set<string> &neededColumnNames,
                       bool isLeft,
                       bool isRight);
  NestedLoopJoinKernel() = default;
  NestedLoopJoinKernel(const NestedLoopJoinKernel&) = default;
  NestedLoopJoinKernel& operator=(const NestedLoopJoinKernel&) = default;

  static NestedLoopJoinKernel make(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                   const set<string> &neededColumnNames,
                                   bool isLeft,
                                   bool isRight);

  tl::expected<void, string> joinIncomingLeft(const shared_ptr<TupleSet> &incomingLeft);
  tl::expected<void, string> joinIncomingRight(const shared_ptr<TupleSet> &incomingRight);

  const std::optional<shared_ptr<TupleSet>> &getBuffer() const;
  const std::optional<shared_ptr<::arrow::Schema>> &getOutputSchema() const;
  tl::expected<void, string> finalize();
  void clearBuffer();

private:
  tl::expected<shared_ptr<TupleSet>, string> join(const shared_ptr<TupleSet> &leftTupleSet,
                                                  const shared_ptr<TupleSet> &rightTupleSet);

  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void bufferOutputSchema(const shared_ptr<TupleSet> &leftTupleSet, const shared_ptr<TupleSet> &rightTupleSet);

  tl::expected<void, string> makeOuterJoinHelpers();
  tl::expected<void, string> computeOuterJoin();

  std::optional<shared_ptr<expression::gandiva::Expression>> predicate_;
  std::optional<shared_ptr<TupleSet>> leftTupleSet_;
  std::optional<shared_ptr<TupleSet>> rightTupleSet_;
  set<string> neededColumnNames_;
  std::optional<shared_ptr<::arrow::Schema>> outputSchema_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in left/right table
  std::optional<shared_ptr<TupleSet>> buffer_;

  bool isLeft_;
  bool isRight_;
  bool isOuterJoinHelperCreated_ = false;
  std::optional<shared_ptr<OuterJoinHelper>> leftJoinHelper_;
  std::optional<shared_ptr<OuterJoinHelper>> rightJoinHelper_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NestedLoopJoinKernel& kernel) {
    return f.object(kernel).fields(f.field("predicate", kernel.predicate_),
                                   f.field("neededColumnNames", kernel.neededColumnNames_),
                                   f.field("isLeft", kernel.isLeft_),
                                   f.field("isRight", kernel.isRight_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINKERNEL_H
