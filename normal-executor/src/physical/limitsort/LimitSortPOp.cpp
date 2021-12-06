//
// Created by Yifei Yang on 12/6/21.
//

#include <normal/executor/physical/limitsort/LimitSortPOp.h>

namespace normal::executor::physical::limitsort {

LimitSortPOp::LimitSortPOp(const string &name,
                           const arrow::compute::SelectKOptions &selectKOptions,
                           const vector<string> &projectColumnNames):
  PhysicalOp(name, "LimitSortPOp", projectColumnNames),
  selectKOptions_(selectKOptions) {}

void LimitSortPOp::onReceive(const Envelope &message) {
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

void LimitSortPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void LimitSortPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    // check if has results
    if (result_.has_value()) {
      // Project using projectColumnNames
      auto expProjectTupleSet = result_.value()->projectExist(getProjectColumnNames());
      if (!expProjectTupleSet) {
        throw std::runtime_error(expProjectTupleSet.error());
      }

      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), name());
      ctx()->tell(tupleMessage);
    }
    ctx()->notifyComplete();
  }
}

void LimitSortPOp::onTuple(const TupleMessage &message) {
  const auto inputTupleSet = makeInput(message.tuples());
  result_ = selectK(inputTupleSet);
}

shared_ptr<TupleSet> LimitSortPOp::makeInput(const shared_ptr<TupleSet> &tupleSet) {
  if (!result_.has_value()) {
    return tupleSet;
  } else {
    const auto &concatenateResult = TupleSet::concatenate({result_.value(), tupleSet});
    if (!concatenateResult.has_value()) {
      throw runtime_error(concatenateResult.error());
    }
    return concatenateResult.value();
  }
}

shared_ptr<TupleSet> LimitSortPOp::selectK(const shared_ptr<TupleSet> &tupleSet) {
  // Compute selectK indices
  const auto table = tupleSet->table();
  const auto &expSelectKIndices = arrow::compute::SelectKUnstable(table, selectKOptions_);
  if (!expSelectKIndices.ok()) {
    throw runtime_error(expSelectKIndices.status().message());
  }
  const auto &selectKIndices = *expSelectKIndices;

  // Make result table using selectKIndices
  const auto expSelectKTable = arrow::compute::Take(table, selectKIndices);
  if (!expSelectKTable.ok()) {
    throw runtime_error(expSelectKTable.status().message());
  }

  return TupleSet::make((*expSelectKTable).table());
}

}