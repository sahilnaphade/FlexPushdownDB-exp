//
// Created by matt on 31/7/20.
//

#include <normal/executor/physical/hashjoin/HashJoinProbeKernel.h>
#include <normal/executor/physical/hashjoin/RecordBatchHashJoiner.h>
#include <arrow/api.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinProbeKernel::HashJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames)) {}

shared_ptr<HashJoinProbeKernel> HashJoinProbeKernel::make(HashJoinPredicate pred, set<string> neededColumnNames) {
  return make_shared<HashJoinProbeKernel>(move(pred), move(neededColumnNames));
}

tl::expected<shared_ptr<normal::tuple::TupleSet>, string>
join(const shared_ptr<RecordBatchHashJoiner> &joiner, const shared_ptr<TupleSet> &tupleSet) {
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Read the table a batch at a time
  auto probeTable = tupleSet->table();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {
    // Join
    auto result = joiner->join(recordBatch);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Get joined result
  auto expectedJoinedTupleSet = joiner->toTupleSet();

#ifndef NDEBUG

  assert(expectedJoinedTupleSet.has_value());
  assert(expectedJoinedTupleSet.value()->valid());
  auto result = expectedJoinedTupleSet.value()->table()->ValidateFull();
  if(!result.ok())
    return tl::make_unexpected(fmt::format("{}, HashJoinProbeKernel", result.message()));

#endif

  return expectedJoinedTupleSet.value();
}

tl::expected<void, string> HashJoinProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex) {

  // Buffer tupleSet if having tuples
  if(tupleSetIndex->size() > 0) {
    auto result = putBuildTupleSetIndex(tupleSetIndex);
    if (!result)
      return tl::make_unexpected(result.error());
  }

  // Check empty
  if (!probeTupleSet_.has_value() || probeTupleSet_.value()->numRows() == 0 || tupleSetIndex->size() == 0) {
    return {};
  }

  // Create output schema
  bufferOutputSchema(tupleSetIndex, probeTupleSet_.value());

  // Create joiner
  auto expectedJoiner = RecordBatchHashJoiner::make(tupleSetIndex,
                                                    pred_.getRightColumnNames(),
                                                    outputSchema_.value(),
                                                    neededColumnIndice_);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join(expectedJoiner.value(), probeTupleSet_.value());
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet) {

  // Buffer tupleSet if having tuples
  if (tupleSet->numRows() > 0) {
    auto result = putProbeTupleSet(tupleSet);
    if (!result)
      return tl::make_unexpected(result.error());
  }

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0 || tupleSet->numRows() == 0) {
    return {};
  }

  // Create output schema
  bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);

  // Create joiner
  auto expectedJoiner = RecordBatchHashJoiner::make(buildTupleSetIndex_.value(),
                                                    pred_.getRightColumnNames(),
                                                    outputSchema_.value(),
                                                    neededColumnIndice_);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

  // Join
  auto expectedJoinedTupleSet = join(expectedJoiner.value(), tupleSet);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value())
    return tl::make_unexpected(bufferResult.error());

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::finalize() {
  clearInput();
  return {};
}
