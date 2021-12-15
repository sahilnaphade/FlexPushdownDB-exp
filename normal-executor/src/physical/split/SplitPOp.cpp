//
// Created by Yifei Yang on 12/13/21.
//

#include <normal/executor/physical/split/SplitPOp.h>
#include <normal/executor/physical/Globals.h>

namespace normal::executor::physical::split {

SplitPOp::SplitPOp(const string &name,
                   const vector<string> &projectColumnNames):
  PhysicalOp(name, "SplitPOp", projectColumnNames) {}

void SplitPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
    this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw runtime_error(fmt::format("Unrecognized message type: {}, {}" + msg.message().type(), name()));
  }
}

void SplitPOp::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  numConsumers: {}", name(), consumers_.size());
}

void SplitPOp::onTuple(const TupleMessage &message) {
  // get tupleSet
  const auto &tupleSet = message.tuples();
  if (tupleSet->numRows() == 0) {
    return;
  }

  // buffer
  const auto &result = bufferInput(tupleSet);
  if (!result.has_value()) {
    throw runtime_error(result.error());
  }

  // send if buffer is large enough
  if (inputTupleSet_.has_value() && inputTupleSet_.value()->numRows() >= DefaultBufferSize * (int) consumers_.size()) {
    splitAndSend();
  }
}

void SplitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    if (inputTupleSet_.has_value() && inputTupleSet_.value()->numRows() > 0) {
      splitAndSend();
    }

    ctx()->notifyComplete();
  }
}

void SplitPOp::produce(const shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);
  consumers_.emplace_back(op->name());
}

tl::expected<void, string> SplitPOp::bufferInput(const shared_ptr<TupleSet>& tupleSet) {
  if (!inputTupleSet_.has_value()) {
    inputTupleSet_ = tupleSet;
    return {};
  }
  const auto &result = inputTupleSet_.value()->append(tupleSet);
  if (!result.has_value()) {
    return result;
  }
  return {};
}

tl::expected<arrow::ArrayVector, string> splitArray(const shared_ptr<arrow::Array> &array, uint n) {
  int64_t splitSize = array->length() / n;
  arrow::ArrayVector splitArrays{n};
  for (uint i = 0; i < n; ++i) {
    int64_t offset = i * splitSize;
    if (i < n - 1) {
      splitArrays[i] = array->Slice(offset, splitSize);
    } else {
      splitArrays[i] = array->Slice(offset);
    }
  }
  return splitArrays;
}

tl::expected<void, string> SplitPOp::splitAndSend() {
  const auto &expTupleSets = split();
  if (!expTupleSets.has_value()) {
    return tl::make_unexpected(expTupleSets.error());
  }
  send(expTupleSets.value());
  clear();
  return {};
}

tl::expected<vector<shared_ptr<TupleSet>>, string> SplitPOp::split() {
  if (!inputTupleSet_.has_value()) {
    return tl::make_unexpected("No input tupleSet to split");
  }

  // combine chunks
  const auto &inputTable = inputTupleSet_.value()->table();
  const auto &expCombinedTable = inputTable->CombineChunks();
  if (!expCombinedTable.ok()) {
    return tl::make_unexpected(expCombinedTable.status().message());
  }
  const auto &combinedTable = expCombinedTable.ValueOrDie();

  // split
  vector<arrow::ArrayVector> outputArrayVectors{consumers_.size()};
  for (const auto &column: combinedTable->columns()) {
    const auto &inputArray = column->chunk(0);
    const auto &expSplitArrays = splitArray(inputArray, consumers_.size());
    if (!expSplitArrays.has_value()) {
      return tl::make_unexpected(expSplitArrays.error());
    }
    const auto &splitArrays = expSplitArrays.value();

    for (uint i = 0; i < consumers_.size(); ++i) {
      outputArrayVectors[i].emplace_back(splitArrays[i]);
    }
  }

  // make tables
  vector<shared_ptr<TupleSet>> outputTupleSets{consumers_.size()};
  const auto &schema = inputTupleSet_.value()->schema();
  for (uint i = 0; i < consumers_.size(); ++i) {
    outputTupleSets[i] = TupleSet::make(schema, outputArrayVectors[i]);
  }
  return outputTupleSets;
}

void SplitPOp::send(const vector<shared_ptr<TupleSet>> &tupleSets) {
  for (uint i = 0; i < consumers_.size(); ++i) {
    const auto &consumer = consumers_[i];
    const auto &tupleSet = tupleSets[i];
    shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
    ctx()->send(tupleMessage, consumer);
  }
}

void SplitPOp::clear() {
  inputTupleSet_ = nullopt;
}

}
