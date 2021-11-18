//
// Created by matt on 7/3/20.
//

#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <utility>

namespace normal::executor::physical::aggregate {

AggregationFunction::AggregationFunction(std::string columnName) : alias_(std::move(columnName)) {}

const std::string &AggregationFunction::alias() const {
  return alias_;
}

}