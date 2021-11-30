//
// Created by matt on 27/10/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEBUILDER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEBUILDER_H

#include <normal/executor/physical/aggregate/AggregationResult.h>
#include <memory>

namespace normal::executor::physical::aggregate {

class AggregateBuilder {
public:
  virtual ~AggregateBuilder() = default;

  virtual tl::expected<void, std::string> append(const std::shared_ptr<AggregationResult> &result) = 0;
  virtual tl::expected<std::shared_ptr<arrow::Array>, std::string> finalise() = 0;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEBUILDER_H
