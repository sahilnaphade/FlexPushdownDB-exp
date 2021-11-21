//
// Created by matt on 7/3/20.
//

#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <utility>

namespace normal::executor::physical::aggregate {

AggregationFunction::AggregationFunction(std::string alias) :
  alias_(std::move(alias)) {}

const std::string &AggregationFunction::alias() const {
  return alias_;
}

}