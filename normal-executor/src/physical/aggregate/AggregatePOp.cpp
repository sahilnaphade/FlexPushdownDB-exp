//
// Created by matt on 11/12/19.
//

#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/aggregate/AggregateResult.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/Message.h>
#include <arrow/scalar.h>
#include <string>
#include <utility>
#include <memory>

namespace normal::executor::physical::aggregate {

AggregatePOp::AggregatePOp(string name,
                           vector<shared_ptr<aggregate::AggregateFunction>> functions,
                           vector<string> projectColumnNames):
  PhysicalOp(move(name), "AggregatePOp", move(projectColumnNames)),
  functions_(move(functions)) {

  // initialize aggregate results
  for (uint i = 0; i < functions_.size(); ++i) {
    aggregateResults_.emplace_back(vector<shared_ptr<AggregateResult>>{});
  }
}

void AggregatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void AggregatePOp::onReceive(const Envelope &message) {
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

void AggregatePOp::onTuple(const TupleMessage &message) {
  compute(message.tuples());
}

void AggregatePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() &&
      this->ctx()->operatorMap().allComplete(normal::executor::physical::POpRelationshipType::Producer)) {

    // Finalize if has result
    if (hasResult()) {
      const auto &tupleSet = finalize();
      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
      ctx()->tell(tupleMessage);
    }

    ctx()->notifyComplete();
  }
}

void AggregatePOp::compute(const shared_ptr<TupleSet> &tupleSet) {
  // compute and save aggregate results
  for (uint i = 0; i < functions_.size(); ++i) {
    const auto &expAggregateResult = functions_[i]->compute(tupleSet);
    if (!expAggregateResult.has_value()) {
      throw runtime_error(expAggregateResult.error());
    }
    aggregateResults_[i].emplace_back(expAggregateResult.value());
  }
}

shared_ptr<TupleSet> AggregatePOp::finalize() {
  // Create output schema
  vector<shared_ptr<arrow::Field>> fields;
  for (const auto &function: functions_) {
    shared_ptr<arrow::Field> field = arrow::field(function->getOutputColumnName(), function->returnType());
    fields.emplace_back(field);
  }
  const auto &schema = arrow::schema(fields);

  // Create output tuple
  vector<shared_ptr<arrow::Array>> columns;
  for (uint i = 0; i < functions_.size(); ++i) {
    const auto &function = functions_[i];
    // Finalize
    const auto &expFinalResult = function->finalize(aggregateResults_[i]);
    if (!expFinalResult.has_value()) {
      throw runtime_error(expFinalResult.error());
    }

    // Make the column of the final result
    const auto &finalResult = expFinalResult.value();
    if (function->returnType() == arrow::int32()) {
      auto colArgh = makeArgh<arrow::Int32Type>(static_pointer_cast<arrow::Int32Scalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else if (function->returnType() == arrow::int64()) {
      auto colArgh = makeArgh<arrow::Int64Type>(static_pointer_cast<arrow::Int64Scalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else if (function->returnType() == arrow::float64()) {
      auto colArgh = makeArgh<arrow::DoubleType>(static_pointer_cast<arrow::DoubleScalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else {
      throw runtime_error("Unsupported aggregate output field type " + function->returnType()->name());
    }
  }

  // Make tupleSet
  const auto &table = arrow::Table::Make(schema, columns);
  const shared_ptr<TupleSet> &aggregatedTuples = TupleSet::make(table);

  // Project using projectColumnNames
  auto expProjectTupleSet = aggregatedTuples->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet) {
    throw runtime_error(expProjectTupleSet.error());
  }

  SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}", aggregatedTuples->toString());
  return expProjectTupleSet.value();
}

bool AggregatePOp::hasResult() {
  return !aggregateResults_[0].empty();
}

}