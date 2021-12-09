//
// Created by matt on 31/7/20.
//

#include <normal/executor/physical/hashjoin/HashJoinProbeKernel2.h>
#include <normal/executor/physical/hashjoin/RecordBatchHashJoiner.h>
#include <arrow/api.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinProbeKernel2::HashJoinProbeKernel2(HashJoinPredicate pred, std::set<std::string> neededColumnNames) :
	pred_(std::move(pred)), neededColumnNames_(std::move(neededColumnNames)) {}

HashJoinProbeKernel2 HashJoinProbeKernel2::make(HashJoinPredicate pred, std::set<std::string> neededColumnNames) {
  return HashJoinProbeKernel2(std::move(pred), std::move(neededColumnNames));
}

tl::expected<void, std::string> HashJoinProbeKernel2::putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {
  const auto &validateRes = validateColumnNames(tupleSetIndex->getTable()->schema(), pred_.getLeftColumnNames());
  if (!validateRes.has_value()) {
    return tl::make_unexpected(fmt::format("Cannot put build tuple set index into probe kernel. {}", validateRes.error()));
  }

  if (!buildTupleSetIndex_.has_value()) {
    buildTupleSetIndex_ = tupleSetIndex;
    return {};
  }
  return buildTupleSetIndex_.value()->merge(tupleSetIndex);
}

tl::expected<void, std::string> HashJoinProbeKernel2::putProbeTupleSet(const std::shared_ptr<TupleSet> &tupleSet) {
  const auto &validateRes = validateColumnNames(tupleSet->schema(), pred_.getRightColumnNames());
  if (!validateRes.has_value()) {
    return tl::make_unexpected(fmt::format("Cannot put probe tuple set into probe kernel. {}", validateRes.error()));
  }

  if (!probeTupleSet_.has_value()) {
    probeTupleSet_ = tupleSet;
    return {};
  }
  return probeTupleSet_.value()->append(tupleSet);
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet>, std::string>
join(const std::shared_ptr<RecordBatchHashJoiner> &joiner, const std::shared_ptr<TupleSet> &tupleSet) {
  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
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

    // join
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
    return tl::make_unexpected(fmt::format("{}, HashJoinProbeKernel2", result.message()));

#endif

  return expectedJoinedTupleSet.value();
}

tl::expected<void, std::string> HashJoinProbeKernel2::joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {

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

tl::expected<void, std::string> HashJoinProbeKernel2::joinProbeTupleSet(const std::shared_ptr<TupleSet> &tupleSet) {

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

tl::expected<void, std::string> HashJoinProbeKernel2::buffer(const std::shared_ptr<TupleSet> &tupleSet) {

  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  }
  else {
    const auto &bufferedTupleSet = buffer_.value();
    const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
    if (!concatenateResult)
      return tl::make_unexpected(concatenateResult.error());

    buffer_ = concatenateResult.value();
  }

  return {};
}

const std::optional<std::shared_ptr<normal::tuple::TupleSet>> &HashJoinProbeKernel2::getBuffer() const {
  return buffer_;
}

void HashJoinProbeKernel2::clear() {
  buffer_ = std::nullopt;
}

void HashJoinProbeKernel2::bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex, const std::shared_ptr<TupleSet> &tupleSet) {
  if (!outputSchema_.has_value()) {

    // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
    std::vector<std::shared_ptr<::arrow::Field>> outputFields;

    for (int c = 0; c < tupleSetIndex->getTable()->schema()->num_fields(); ++c) {
      auto field = tupleSetIndex->getTable()->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(std::make_shared<std::pair<bool, int>>(true, c));
        outputFields.emplace_back(field);
      }
    }
    for (int c = 0; c < tupleSet->schema()->num_fields(); ++c) {
      auto field = tupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(std::make_shared<std::pair<bool, int>>(false, c));
        outputFields.emplace_back(field);
      }
    }
    outputSchema_ = std::make_shared<::arrow::Schema>(outputFields);
  }
}

tl::expected<void, std::string> HashJoinProbeKernel2::validateColumnNames(const std::shared_ptr<arrow::Schema> &schema,
                                                                          const std::vector<std::string> &columnNames) {
  for (const auto &columnName: columnNames) {
    if(schema->GetFieldIndex(columnName) == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' does not exist.", columnName));
    }
  }

  return {};
}
