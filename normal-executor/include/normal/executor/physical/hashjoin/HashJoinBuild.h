//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILD_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILD_H

#include <normal/executor/physical/hashjoin/HashJoinBuildKernel2.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <arrow/scalar.h>
#include <unordered_map>

using namespace normal::executor::message;

namespace normal::executor::physical::hashjoin {

/**
 * Operator implementing build phase of a hash join
 *
 * Builds a hash table of tuples from one of the relations in a join (ideally the smaller relation). That hashtable is
 * then used in the probe phase to select rows to add to the final joined relation.
 *
 */
class HashJoinBuild : public PhysicalOp {

public:
  explicit HashJoinBuild(const std::string &name, std::string columnName, long queryId = 0);

  static std::shared_ptr<HashJoinBuild> create(const std::string &name, const std::string &columnName);

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

  [[nodiscard]] tl::expected<void, std::string> buffer(const std::shared_ptr<TupleSet2>& tupleSet);
  void send(bool force);

  /**
   * The column to hash on
   */
  std::string columnName_;

  HashJoinBuildKernel2 kernel_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILD_H
