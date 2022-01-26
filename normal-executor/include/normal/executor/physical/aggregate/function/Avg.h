//
// Created by Yifei Yang on 1/25/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>

namespace normal::executor::physical::aggregate {
  
class Avg : public AggregateFunction {
  
public:
  Avg(const string &outputColumnName,
      const shared_ptr<normal::expression::gandiva::Expression> &expression);
  Avg() = default;
  Avg(const Avg&) = default;
  Avg& operator=(const Avg&) = default;

  shared_ptr<arrow::DataType> returnType() const override;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

private:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Avg& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
