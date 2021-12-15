//
// Created by Yifei Yang on 12/12/21.
//

#include <normal/executor/physical/nestedloopjoin/NestedLoopJoinKernel.h>
#include <normal/executor/physical/nestedloopjoin/RecordBatchNestedLoopJoiner.h>

namespace normal::executor::physical::nestedloopjoin {

NestedLoopJoinKernel::NestedLoopJoinKernel(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                           const set<string> &neededColumnNames):
  predicate_(predicate),
  neededColumnNames_(neededColumnNames) {}

NestedLoopJoinKernel NestedLoopJoinKernel::make(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                const set<string> &neededColumnNames) {
  return NestedLoopJoinKernel(predicate, neededColumnNames);
}

tl::expected<void, string> bufferInput(optional<shared_ptr<TupleSet>> &buffer,
                                       const shared_ptr<TupleSet> &incomingTupleSet) {
  if (!buffer.has_value()) {
    buffer = incomingTupleSet;
    return {};
  }
  return buffer.value()->append(incomingTupleSet);
}

tl::expected<shared_ptr<TupleSet>, string> NestedLoopJoinKernel::join(const shared_ptr<TupleSet> &leftTupleSet,
                                                                      const shared_ptr<TupleSet> &rightTupleSet) {
  // create joiner
  const auto &joiner = RecordBatchNestedLoopJoiner::make(predicate_, outputSchema_.value(), neededColumnIndice_);

  ::arrow::Status status;

  // read the left table a batch at a time
  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> leftRecordBatchResult;
  ::arrow::TableBatchReader leftReader{*leftTupleSet->table()};
  leftReader.set_chunksize((int64_t) DefaultChunkSize);

  // read a left batch
  leftRecordBatchResult = leftReader.Next();
  if (!leftRecordBatchResult.ok()) {
    return tl::make_unexpected(leftRecordBatchResult.status().message());
  }
  auto leftRecordBatch = *leftRecordBatchResult;

  while (leftRecordBatch) {
    // read the right table a batch at a time
    ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> rightRecordBatchResult;
    ::arrow::TableBatchReader rightReader{*rightTupleSet->table()};
    rightReader.set_chunksize((int64_t) DefaultChunkSize);

    // read a right batch
    rightRecordBatchResult = rightReader.Next();
    if (!rightRecordBatchResult.ok()) {
      return tl::make_unexpected(rightRecordBatchResult.status().message());
    }
    auto rightRecordBatch = *rightRecordBatchResult;

    while (rightRecordBatch) {
      // join
      auto result = joiner->join(leftRecordBatch, rightRecordBatch);
      if (!result.has_value()) {
        return tl::make_unexpected(result.error());
      }

      // read a right batch
      rightRecordBatchResult = rightReader.Next();
      if (!rightRecordBatchResult.ok()) {
        return tl::make_unexpected(rightRecordBatchResult.status().message());
      }
      rightRecordBatch = *rightRecordBatchResult;
    }

    // read a left batch
    leftRecordBatchResult = leftReader.Next();
    if (!leftRecordBatchResult.ok()) {
      return tl::make_unexpected(leftRecordBatchResult.status().message());
    }
    leftRecordBatch = *leftRecordBatchResult;
  }

  return joiner->toTupleSet();
}

tl::expected<void, string> NestedLoopJoinKernel::joinIncomingLeft(const shared_ptr<TupleSet> &incomingLeft) {
  // buffer tupleSet if having tuples
  if (incomingLeft->numRows() > 0) {
    auto result = bufferInput(leftTupleSet_, incomingLeft);
    if (!result) {
      return tl::make_unexpected(result.error());
    }
  }
  
  // check empty
  if (!rightTupleSet_.has_value() || rightTupleSet_.value()->numRows() == 0 || incomingLeft->numRows() == 0) {
    return {};
  }

  // create output schema
  bufferOutputSchema(incomingLeft, rightTupleSet_.value());

  // join
  const auto &expectedJoinedTupleSet = join(incomingLeft, rightTupleSet_.value());
  if (!expectedJoinedTupleSet.has_value()) {
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }

  // buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value()) {
    return tl::make_unexpected(bufferResult.error());
  }

  return {};
}

tl::expected<void, string> NestedLoopJoinKernel::joinIncomingRight(const shared_ptr<TupleSet> &incomingRight) {
  // buffer tupleSet if having tuples
  if (incomingRight->numRows() > 0) {
    auto result = bufferInput(rightTupleSet_, incomingRight);
    if (!result) {
      return tl::make_unexpected(result.error());
    }
  }

  // check empty
  if (!leftTupleSet_.has_value() || leftTupleSet_.value()->numRows() == 0 || incomingRight->numRows() == 0) {
    return {};
  }

  // create output schema
  bufferOutputSchema(leftTupleSet_.value(), incomingRight);

  // join
  const auto &expectedJoinedTupleSet = join(leftTupleSet_.value(), incomingRight);
  if (!expectedJoinedTupleSet.has_value()) {
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }

  // buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value()) {
    return tl::make_unexpected(bufferResult.error());
  }

  return {};
}

tl::expected<void, string> NestedLoopJoinKernel::buffer(const shared_ptr<TupleSet> &tupleSet) {
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

const optional<shared_ptr<TupleSet>> &NestedLoopJoinKernel::getBuffer() const {
  return buffer_;
}

void NestedLoopJoinKernel::clear() {
  buffer_ = nullopt;
}

void NestedLoopJoinKernel::bufferOutputSchema(const shared_ptr<TupleSet> &leftTupleSet, 
                                              const shared_ptr<TupleSet> &rightTupleSet) {
  if (!outputSchema_.has_value()) {
    // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
    vector<shared_ptr<::arrow::Field>> outputFields;

    for (int c = 0; c < leftTupleSet->schema()->num_fields(); ++c) {
      auto field = leftTupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(true, c));
        outputFields.emplace_back(field);
      }
    }
    for (int c = 0; c < rightTupleSet->schema()->num_fields(); ++c) {
      auto field = rightTupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(false, c));
        outputFields.emplace_back(field);
      }
    }
    outputSchema_ = make_shared<::arrow::Schema>(outputFields);
  }
}

}
