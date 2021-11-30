//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL2_H

#include <normal/executor/physical/hashjoin/HashJoinPredicate.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/TupleSetIndex.h>
#include <set>
#include <memory>

using namespace normal::tuple;

namespace normal::executor::physical::hashjoin {

class HashJoinProbeKernel2 {

public:
  explicit HashJoinProbeKernel2(HashJoinPredicate pred, std::set<std::string> neededColumnNames);
  static HashJoinProbeKernel2 make(HashJoinPredicate pred, std::set<std::string> neededColumnNames);

  tl::expected<void, std::string> joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex>& tupleSetIndex);
  tl::expected<void, std::string> joinProbeTupleSet(const std::shared_ptr<TupleSet>& tupleSet);
  [[nodiscard]] const std::optional<std::shared_ptr<normal::tuple::TupleSet>> &getBuffer() const;
  void clear();

  [[nodiscard]] tl::expected<void, std::string> putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex>& tupleSetIndex);
  [[nodiscard]] tl::expected<void, std::string> putProbeTupleSet(const std::shared_ptr<TupleSet>& tupleSet);
private:
  HashJoinPredicate pred_;
  std::optional<std::shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
  std::optional<std::shared_ptr<TupleSet>> probeTupleSet_;
  std::set<std::string> neededColumnNames_;
  std::optional<std::shared_ptr<::arrow::Schema>> outputSchema_;
  std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in build/probe table
  std::optional<std::shared_ptr<normal::tuple::TupleSet>> buffer_;

  [[nodiscard]] tl::expected<void, std::string> buffer(const std::shared_ptr<TupleSet>& tupleSet);
  void bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex, const std::shared_ptr<TupleSet> &tupleSet);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL2_H
