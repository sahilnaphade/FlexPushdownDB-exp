//
// Created by Yifei Yang on 12/3/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>

namespace normal::executor::physical::aggregate {

class Count : public AggregateFunction {

public:
  Count(const string &outputColumnName,
        const shared_ptr<normal::expression::gandiva::Expression> &expression);
  Count() = default;
  Count(const Count&) = default;
  Count& operator=(const Count&) = default;

  shared_ptr<arrow::DataType> returnType() override;

  set<string> involvedColumnNames() override;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

private:
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Count& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H
