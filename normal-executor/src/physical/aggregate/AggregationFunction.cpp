//
// Created by matt on 7/3/20.
//

#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <utility>

namespace normal::executor::physical::aggregate {

AggregationFunction::AggregationFunction(std::string alias,
                                         std::shared_ptr<normal::expression::gandiva::Expression> expression) :
  alias_(std::move(alias)),
  expression_(std::move(expression)) {}

const std::string &AggregationFunction::getAlias() const {
  return alias_;
}

const std::shared_ptr<normal::expression::gandiva::Expression> &AggregationFunction::getExpression() const {
  return expression_;
}

}