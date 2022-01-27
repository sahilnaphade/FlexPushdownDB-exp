//
// Created by Yifei Yang on 1/26/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>

namespace normal::executor::physical::aggregate {

/**
 * Abstract base class derived by Avg and AvgReduce
 */
class AvgBase: public AggregateFunction {

public:
  AvgBase(AggregateFunctionType type,
          const string &outputColumnName,
          const shared_ptr<normal::expression::gandiva::Expression> &expression);
  AvgBase() = default;
  AvgBase(const AvgBase&) = default;
  AvgBase& operator=(const AvgBase&) = default;

  shared_ptr<arrow::DataType> returnType() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

protected:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H
