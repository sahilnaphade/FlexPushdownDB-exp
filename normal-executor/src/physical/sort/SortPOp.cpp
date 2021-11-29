//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/sort/SortPOp.h>

namespace normal::executor::physical::sort {

SortPOp::SortPOp(const string &name,
                 const arrow::compute::SortOptions &sortOptions,
                 const vector<string> &projectColumnNames,
                 long queryId) :
  PhysicalOp(name, "SortPOp", projectColumnNames, queryId),
  sortOptions_(sortOptions) {}

void SortPOp::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw runtime_error("Unrecognized message type " + message.message().type());
  }
}

void SortPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void SortPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    const auto &sortedTupleSet = sort();

    // Project using projectColumnNames
    auto expProjectTupleSet = sortedTupleSet->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      throw std::runtime_error(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), name());
    ctx()->tell(tupleMessage);
    ctx()->notifyComplete();
  }
}

void SortPOp::onTuple(const TupleMessage &message) {
  buffer(message.tuples());
}

void SortPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  } else {
    const auto &concatenateResult = TupleSet::concatenate({buffer_.value(), tupleSet});
    if (!concatenateResult.has_value()) {
      throw runtime_error(concatenateResult.error());
    }
    buffer_ = concatenateResult.value();
  }
}

shared_ptr<TupleSet> SortPOp::sort() {
  if (!buffer_.has_value()) {
    return TupleSet::makeWithEmptyTable();
  }

  // Compute sort indices
  const auto table = buffer_.value()->table();
  const auto &expSortIndices = arrow::compute::SortIndices(table, sortOptions_);
  if (!expSortIndices.ok()) {
    throw runtime_error(expSortIndices.status().message());
  }
  const auto &sortIndices = *expSortIndices;

  // Make sorted table using sort indices
  const auto expSortedTable = arrow::compute::Take(table, sortIndices);
  if (!expSortedTable.ok()) {
    throw runtime_error(expSortedTable.status().message());
  }

  return TupleSet::make((*expSortedTable).table());
}

}
