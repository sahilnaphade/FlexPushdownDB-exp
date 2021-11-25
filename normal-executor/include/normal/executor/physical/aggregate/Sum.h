//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_SUM_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_SUM_H

#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <normal/executor/message/TupleMessage.h>

namespace normal::executor::physical::aggregate {

class Sum : public AggregationFunction {

private:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";

public:
  Sum(std::string alias, std::shared_ptr<normal::expression::gandiva::Expression> expression);
  ~Sum() override = default;

  void apply(std::shared_ptr<aggregate::AggregationResult> result, std::shared_ptr<TupleSet> tuples) override;
  std::shared_ptr<arrow::DataType> returnType() override;

  void finalize(std::shared_ptr<aggregate::AggregationResult> result) override;

  void cacheInputSchema(const TupleSet &tuples);
  void buildAndCacheProjector();
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_SUM_H
