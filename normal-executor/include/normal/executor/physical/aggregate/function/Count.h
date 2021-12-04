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

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

private:
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H
