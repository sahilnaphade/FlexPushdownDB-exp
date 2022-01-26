//
// Created by Yifei Yang on 12/14/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>

namespace normal::executor::physical::aggregate {

/**
 * As arrow computes min and max in the same api together, we here also make them the same class
 */
class MinMax : public AggregateFunction {

public:
  MinMax(bool isMin,
         const string &outputColumnName,
         const shared_ptr<normal::expression::gandiva::Expression> &expression);
  MinMax() = default;
  MinMax(const MinMax&) = default;
  MinMax& operator=(const MinMax&) = default;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

private:
  constexpr static const char *const MIN_RESULT_KEY = "MIN";
  constexpr static const char *const MAX_RESULT_KEY = "MAX";

  bool isMin_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, MinMax& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_),
                                 f.field("isMin", func.isMin_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H
