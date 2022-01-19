//
// Created by Yifei Yang on 12/6/21.
//

#include <normal/executor/physical/limitsort/LimitSortPOp.h>

namespace normal::executor::physical::limitsort {

LimitSortPOp::LimitSortPOp(const string &name,
                           const vector<string> &projectColumnNames,
                           int nodeId,
                           int64_t k,
                           const vector<SortKey> &sortKeys):
  PhysicalOp(name, LIMIT_SORT, projectColumnNames, nodeId),
  k_(k),
  sortKeys_(sortKeys) {}

std::string LimitSortPOp::getTypeString() const {
  return "LimitSortPOp";
}

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

void LimitSortPOp::makeArrowSelectKOptions() {
  if (!arrowSelectKOptions_.has_value()) {
    vector<arrow::compute::SortKey> arrowSortKeys;
    for (const auto &sortKey: sortKeys_) {
      const auto &name = sortKey.getName();
      auto direction = sortKey.getOrder() == plan::prephysical::ASCENDING ?
                       arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending;
      arrowSortKeys.emplace_back(arrow::compute::SortKey(name, direction));
    }
    arrowSelectKOptions_ = arrow::compute::SelectKOptions(k_, arrowSortKeys);
  }
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
  // arrow api will crash if table has no rows, so we need a check
  if (tupleSet->numRows() == 0) {
    return tupleSet;
  }

  // Make selectKOptions if not yet
  makeArrowSelectKOptions();

  // Compute selectK indices
  const auto table = tupleSet->table();
  if (!arrowSelectKOptions_.has_value()) {
    throw runtime_error("Arrow SelectKOptions not set yet");
  }
  const auto &expSelectKIndices = arrow::compute::SelectKUnstable(table, *arrowSelectKOptions_);
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
