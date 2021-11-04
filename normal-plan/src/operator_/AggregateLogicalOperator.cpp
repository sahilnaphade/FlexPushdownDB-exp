//
// Created by matt on 2/4/20.
//

#include <normal/plan/operator_/AggregateLogicalOperator.h>

#include <normal/pushdown/aggregate/Aggregate.h>
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Divide.h>
#include <normal/expression/gandiva/Add.h>
#include <normal/expression/gandiva/Subtract.h>

using namespace normal::pushdown::aggregate;
using namespace normal::plan::operator_;
using namespace normal::expression;

AggregateLogicalOperator::AggregateLogicalOperator(
	std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions,
	std::shared_ptr<LogicalOperator> producer)
    : LogicalOperator(type::OperatorTypes::aggregateOperatorType()),
    functions_(std::move(functions)),
    producer_(std::move(producer)) {}


std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> AggregateLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();

  for (auto index = 0; index < numConcurrentUnits_; index++) {
    auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    for (const auto &function: *functions_) {
      expressions->emplace_back(function->toExecutorFunction());
    }

    // FIXME: Defaulting to name -> aggregation
    auto aggregate = std::make_shared<Aggregate>(fmt::format("aggregate-{}", index),
                                                                   expressions,
                                                                   getQueryId());
    operators->emplace_back(aggregate);
  }

  // add aggregate reduce if needed
  if (numConcurrentUnits_ > 1) {
    auto reduceExpressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    for (const auto &function: *functions_) {
      reduceExpressions->emplace_back(function->toExecutorReduceFunction());
    }
    auto aggregateReduce = std::make_shared<Aggregate>("aggregateReduce", reduceExpressions, getQueryId());

    // wire up internally
    for (const auto &aggregate: *operators) {
      aggregate->produce(aggregateReduce);
      aggregateReduce->consume(aggregate);
    }
    operators->emplace_back(aggregateReduce);
  }

  return operators;
}

const std::shared_ptr<LogicalOperator> &AggregateLogicalOperator::getProducer() const {
  return producer_;
}

void AggregateLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  numConcurrentUnits_ = numConcurrentUnits;
}
