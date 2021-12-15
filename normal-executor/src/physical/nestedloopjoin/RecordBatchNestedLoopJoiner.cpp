//
// Created by Yifei Yang on 12/13/21.
//

#include <normal/executor/physical/nestedloopjoin/RecordBatchNestedLoopJoiner.h>
#include <normal/expression/gandiva/Filter.h>
#include <normal/tuple/ArrayAppenderWrapper.h>

namespace normal::executor::physical::nestedloopjoin {

RecordBatchNestedLoopJoiner::RecordBatchNestedLoopJoiner(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                         const shared_ptr<::arrow::Schema> &outputSchema,
                                                         const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice):
  predicate_(predicate),
  outputSchema_(outputSchema),
  joinedArrayVectors_{static_cast<uint>(outputSchema_->num_fields())} {

  // initialize required variables
  arrow::FieldVector leftFields, rightFields;
  for (uint c = 0; c < neededColumnIndice.size(); ++c) {
    const auto &pair = neededColumnIndice[c];
    const auto &field = outputSchema->field((int) c);
    if (pair->first) {
      neededLeftColumnIndexes_.emplace_back(pair->second);
      leftFields.emplace_back(field);
    } else {
      neededRightColumnIndexes_.emplace_back(pair->second);
      rightFields.emplace_back(field);
    }
  }
  leftOutputSchema_ = arrow::schema(leftFields);
  rightOutputSchema_ = arrow::schema(rightFields);
}

shared_ptr<RecordBatchNestedLoopJoiner>
RecordBatchNestedLoopJoiner::make(const optional<shared_ptr<Expression>> &predicate,
                                  const shared_ptr<::arrow::Schema> &outputSchema,
                                  const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice) {
  return make_shared<RecordBatchNestedLoopJoiner>(predicate, outputSchema, neededColumnIndice);
}

tl::expected<void, string> RecordBatchNestedLoopJoiner::join(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                                             const shared_ptr<::arrow::RecordBatch> &rightRecordBatch) {
  // compute cartesian product
  const auto &expCartesianBatch = cartesian(leftRecordBatch, rightRecordBatch);
  if (!expCartesianBatch.has_value()) {
    return tl::make_unexpected(expCartesianBatch.error());
  }
  const auto &cartesianBatch = expCartesianBatch.value();

  // filter using predicate
  arrow::ArrayVector outputArrayVector;
  if (predicate_.has_value()) {
    outputArrayVector = filter(cartesianBatch);
  } else {
    outputArrayVector = cartesianBatch->columns();
  }

  // buffer
  for (uint c = 0; c < outputArrayVector.size(); ++c) {
    joinedArrayVectors_[c].emplace_back(outputArrayVector[c]);
  }

  return {};
}

tl::expected<shared_ptr<arrow::RecordBatch>, string>
RecordBatchNestedLoopJoiner::cartesian(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                       const shared_ptr<::arrow::RecordBatch> &rightRecordBatch) {
  arrow::Status status;
  vector<shared_ptr<arrow::Array>> outputArrays;
  optional<int64_t> outputNumRows = nullopt;

  // create column references
  const auto &leftColumns = leftRecordBatch->columns();
  const auto &rightColumns = rightRecordBatch->columns();

  /*
   * left part: add whole left array [#rows of right array] times
   */
  vector<arrow::ArrayVector> leftOutputArrayVectors{neededLeftColumnIndexes_.size()};
  for (int64_t rr = 0; rr < rightRecordBatch->num_rows(); ++rr) {
    for (uint c = 0; c < neededLeftColumnIndexes_.size(); ++c) {
      const auto &leftColumn = leftColumns[neededLeftColumnIndexes_[c]];
      const auto &copiedArray = leftColumn->Slice(0);
      leftOutputArrayVectors[c].emplace_back(copiedArray);
    }
  }

  // combine array vectors to single arrays
  vector<shared_ptr<arrow::ChunkedArray>> leftOutputChunkedArrays;
  for (uint c = 0; c < neededLeftColumnIndexes_.size(); ++c) {
    leftOutputChunkedArrays.emplace_back(make_shared<arrow::ChunkedArray>(leftOutputArrayVectors[c]));
  }
  const auto &leftOutputTable = arrow::Table::Make(leftOutputSchema_, leftOutputChunkedArrays);
  const auto &result = leftOutputTable->CombineChunks();
  if (!result.ok()) {
    return tl::make_unexpected(result.status().message());
  }
  for (const auto &column: result.ValueOrDie()->columns()) {
    const auto &array = column->chunk(0);
    // check whether num_rows is consistent
    if (!outputNumRows.has_value()) {
      outputNumRows = array->length();
    } else {
      if (outputNumRows.value() != array->length()) {
        return tl::make_unexpected(fmt::format("Inconsistent num_rows of output columns during cartesian product, "
                                               "should be {}, but get {}", outputNumRows.value(), array->length()));
      }
    }
    outputArrays.emplace_back(column->chunk(0));
  }

  /*
   * right part: append values regularly
   */
  // create right appenders
  vector<shared_ptr<ArrayAppender>> rightAppenders{(uint) rightOutputSchema_->num_fields()};
  for (int c = 0; c < rightOutputSchema_->num_fields(); ++c) {
    auto expectedAppender = ArrayAppenderBuilder::make(rightOutputSchema_->field(c)->type());
    if (!expectedAppender.has_value())
      return tl::make_unexpected(expectedAppender.error());
    rightAppenders[c] = expectedAppender.value();
  }

  // append right values
  for (int64_t lr = 0; lr < leftRecordBatch->num_rows(); ++lr) {
    for (int64_t rr = 0; rr < rightRecordBatch->num_rows(); ++rr) {
      for (int c = 0; c < rightOutputSchema_->num_fields(); ++c) {
        int colId = neededRightColumnIndexes_[c];
        rightAppenders[c]->safeAppendValue(rightColumns[colId], (size_t) rr);
      }
    }
  }

  // finalize right arrays
  for (const auto &rightAppender: rightAppenders) {
    auto expectedArray = rightAppender->finalize();
    if (!expectedArray.has_value()) {
      return tl::make_unexpected(expectedArray.error());
    }
    const auto &array = expectedArray.value();

    // check whether num_rows is consistent
    if (!outputNumRows.has_value()) {
      outputNumRows = array->length();
    } else {
      if (outputNumRows.value() != array->length()) {
        return tl::make_unexpected(fmt::format("Inconsistent num_rows of output columns during cartesian product, "
                                               "should be {}, but get {}", outputNumRows.value(), array->length()));
      }
    }
    outputArrays.emplace_back(array);
  }

  // create output record batch
  if (!outputNumRows.has_value()) {
    return tl::make_unexpected("No output num_rows");
  }
  return arrow::RecordBatch::Make(outputSchema_, outputNumRows.value(), outputArrays);
}

arrow::ArrayVector RecordBatchNestedLoopJoiner::filter(const shared_ptr<arrow::RecordBatch> &recordBatch) {
  // build filter if not yet
  if (!filter_.has_value()){
    filter_ = normal::expression::gandiva::Filter::make(predicate_.value());
    filter_.value()->compile(Schema::make(recordBatch->schema()));
  }

  // filter the record batch
  return filter_.value()->evaluate(*recordBatch);
}

tl::expected<shared_ptr<TupleSet>, string> RecordBatchNestedLoopJoiner::toTupleSet() {
  arrow::Status status;

  // make chunked arrays
  vector<shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
  for (const auto &joinedArrayVector: joinedArrayVectors_) {
    // check empty
    if (joinedArrayVector.empty()) {
      return TupleSet::make(outputSchema_);
    }

    auto chunkedArray = make_shared<::arrow::ChunkedArray>(joinedArrayVector);
    chunkedArrays.emplace_back(chunkedArray);
  }

  auto joinedTable = ::arrow::Table::Make(outputSchema_, chunkedArrays);
  auto joinedTupleSet = TupleSet::make(joinedTable);

  joinedArrayVectors_.clear();
  return joinedTupleSet;
}

}
