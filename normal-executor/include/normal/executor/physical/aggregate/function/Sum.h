//
// Created by Yifei Yang on 12/2/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_SUM_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_SUM_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>

namespace normal::executor::physical::aggregate {

class Sum : public AggregateFunction {

public:
  Sum(const string &outputColumnName,
      const shared_ptr<normal::expression::gandiva::Expression> &expression);
  Sum() = default;
  Sum(const Sum&) = default;
  Sum& operator=(const Sum&) = default;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

private:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Sum& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_SUM_H
