//
// Created by matt on 6/5/20.
//

#include <normal/executor/physical/filter/FilterPOp.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/cache/WeightRequestMessage.h>
#include <normal/expression/gandiva/Filter.h>
#include <normal/expression/gandiva/BinaryExpression.h>
#include <normal/tuple/Globals.h>
#include <utility>

using namespace normal::executor::physical::filter;
using namespace normal::cache;
using namespace normal::expression::gandiva;

FilterPOp::FilterPOp(std::string name,
               std::vector<std::string> projectColumnNames,
               int nodeId,
               std::shared_ptr<Expression> predicate,
               std::shared_ptr<Table> table,
               std::vector<std::shared_ptr<SegmentKey>> weightedSegmentKeys) :
	PhysicalOp(std::move(name), FILTER, std::move(projectColumnNames), nodeId),
  predicate_(std::move(predicate)),
	received_(normal::tuple::TupleSet::makeWithNullTable()),
	filtered_(normal::tuple::TupleSet::makeWithNullTable()),
  table_(std::move(table)),
	weightedSegmentKeys_(std::move(weightedSegmentKeys)) {}

std::string FilterPOp::getTypeString() const {
  return "FilterPOp";
}

void FilterPOp::onReceive(const Envelope &Envelope) {
  const auto& message = Envelope.message();

  if (message.type() == "StartMessage") {
	  this->onStart();
  } else if (message.type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message);
    this->onTuple(tupleMessage);
  } else if (message.type() == "CompleteMessage") {
    if (*applicable_) {
      auto completeMessage = dynamic_cast<const CompleteMessage &>(message);
      this->onComplete(completeMessage);
    }
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error("Unrecognized message type " + message.type());
  }
}

void FilterPOp::onStart() {
  assert(received_->validate());
  assert(filtered_->validate());
}

void FilterPOp::onTuple(const TupleMessage &Message) {
  SPDLOG_DEBUG("onTuple  |  Message tupleSet - numRows: {}", Message.tuples()->numRows());
  /**
   * Check if this filter is applicable, if not, just send an empty table and complete
   */
  const auto& tupleSet = Message.tuples();
  if (applicable_ == nullptr) {
    applicable_ = std::make_shared<bool>(isApplicable(tupleSet));
  }

  if (*applicable_) {
    bufferTuples(tupleSet);
    SPDLOG_DEBUG("FilterPOp onTuple: {}, {}, {}", tupleSet->numRows(), received_->numRows(), name());
    buildFilter();
    if (received_->numRows() > DefaultBufferSize) {
      filterTuples();
      sendTuples();
    }
  } else {
    // empty table
    auto emptyTupleSet = normal::tuple::TupleSet::makeWithEmptyTable();
    std::shared_ptr<normal::executor::message::Message> tupleMessage = std::make_shared<TupleMessage>(emptyTupleSet,
                                                                                                      name());
    ctx()->tell(tupleMessage);
    ctx()->notifyComplete();
  }
}

void FilterPOp::onComplete(const CompleteMessage&) {
  SPDLOG_DEBUG("onComplete  |  Received buffer tupleSet - numRows: {}", received_->numRows());

  if(received_->valid()) {
    filterTuples();
    sendTuples();
  }

  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    if (!weightedSegmentKeys_.empty() && totalNumRows_ > 0 && *applicable_) {
      sendSegmentWeight();
    }

    filter_ = std::nullopt;
    assert(this->received_->numRows() == 0);
	  assert(this->filtered_->numRows() == 0);

	  ctx()->notifyComplete();
  }
}

void FilterPOp::bufferTuples(const std::shared_ptr<normal::tuple::TupleSet>& tupleSet) {
  if(!received_->valid()) {
    received_ = tupleSet;
  } else {
    auto result = received_->append(tupleSet);
    if (!result.has_value()) {
      throw std::runtime_error(result.error());
    }
    assert(received_->validate());
  }
}

bool FilterPOp::isApplicable(const std::shared_ptr<normal::tuple::TupleSet>& tupleSet) {
  auto predicateColumnNames = predicate_->involvedColumnNames();
  auto tupleColumnNames = std::make_shared<std::vector<std::string>>();
  for (auto const &field: tupleSet->schema()->fields()) {
    tupleColumnNames->emplace_back(field->name());
  }

  for (auto const &columnName: predicateColumnNames) {
    if (std::find(tupleColumnNames->begin(), tupleColumnNames->end(), columnName) == tupleColumnNames->end()) {
      return false;
    }
  }
  return true;
}

void FilterPOp::buildFilter() {
  if(!filter_.has_value()){
	filter_ = Filter::make(predicate_);
	filter_.value()->compile(Schema::make(received_->schema()));
  }
}

void FilterPOp::filterTuples() {
  // input metrics
  if (recordSpeeds) {
    totalBytesFiltered_ += received_->size();
  }
  totalNumRows_ += received_->numRows();
  inputBytesFiltered_ += received_->size();

  // do filter
  std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();
  filtered_ = filter_.value()->evaluate(*received_);
  assert(filtered_->validate());
  std::chrono::steady_clock::time_point stopTime = std::chrono::steady_clock::now();
  filterTimeNS_ += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  // output metrics
  filteredNumRows_ += filtered_->numRows();
  outputBytesFiltered_ += filtered_->size();

  received_->clear();
  assert(received_->validate());
}

void FilterPOp::sendTuples() {
  // Project using projectColumnNames
  auto expProjectTupleSet = TupleSet::make(filtered_->table())->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet) {
    throw std::runtime_error(expProjectTupleSet.error());
  }

  std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(expProjectTupleSet.value(),name());
  ctx()->tell(tupleMessage);
  filtered_->clear();
  assert(filtered_->validate());
}

int getPredicateNum(const std::shared_ptr<Expression> &expr) {
  if (expr->getType() == AND || expr->getType() == OR) {
    auto biExpr = std::static_pointer_cast<BinaryExpression>(expr);
    return getPredicateNum(biExpr->getLeft()) + getPredicateNum(biExpr->getRight());
  } else {
    return 1;
  }
}

void FilterPOp::sendSegmentWeight() {
  /**
   * Weight function:
   *   w = sel / vNetwork + (lenRow / (lenCol * vScan) + #pred / (lenCol * vFilterPOp)) / #key
   */
  auto selectivity = ((double) filteredNumRows_) / ((double ) totalNumRows_);
  auto predicateNum = (double) getPredicateNum(predicate_);
  auto numKey = (double) weightedSegmentKeys_.size();
  std::unordered_map<std::shared_ptr<SegmentKey>, double> weightMap;
  for (auto const &segmentKey: weightedSegmentKeys_) {
    auto columnName = segmentKey->getColumnName();
    auto lenCol = (double) table_->getApxColumnLength(columnName);
    auto lenRow = (double) table_->getApxRowLength();

    auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan) + predicateNum / (lenCol * vS3Filter)) / numKey;
    weightMap.emplace(segmentKey, weight);
  }

  ctx()->send(WeightRequestMessage::make(weightMap, name()), "SegmentCache")
          .map_error([](auto err) { throw std::runtime_error(err); });
}

size_t FilterPOp::getFilterTimeNS() const {
  return filterTimeNS_;
}

size_t FilterPOp::getFilterInputBytes() const {
  return inputBytesFiltered_;
}

size_t FilterPOp::getFilterOutputBytes() const {
  return outputBytesFiltered_;
}
