//
// Created by Yifei Yang on 12/2/21.
//

#include <normal/executor/physical/aggregate/function/Sum.h>
#include <normal/tuple/ColumnBuilder.h>
#include <arrow/compute/api_aggregate.h>

namespace normal::executor::physical::aggregate {

Sum::Sum(const string &outputColumnName,
         const shared_ptr<normal::expression::gandiva::Expression> &expression)
  : AggregateFunction(outputColumnName, expression) {}

tl::expected<shared_ptr<AggregateResult>, string> Sum::compute(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &aggChunkedArray = evaluateExpr(tupleSet);

  // compute the aggregation
  const auto &expResultScalar = arrow::compute::Sum(aggChunkedArray);
  if (!expResultScalar.ok()) {
    return tl::make_unexpected(expResultScalar.status().message());
  }
  const auto &resultScalar = *expResultScalar;

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(SUM_RESULT_KEY, resultScalar.scalar());
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Sum::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  unique_ptr<arrow::ArrayBuilder> arrayBuilder;
  arrow::Status status = arrow::MakeBuilder(arrow::default_memory_pool(), returnType_, &arrayBuilder);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  for (const auto &aggregateResult: aggregateResults) {
    const auto &expResultScalar = aggregateResult->get(SUM_RESULT_KEY);
    if (!expResultScalar.has_value()) {
      return tl::make_unexpected(fmt::format("Aggregate result key not found: {}", SUM_RESULT_KEY));
    }
    status = arrayBuilder->AppendScalar(*expResultScalar.value());
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  }
  const auto &expArray = arrayBuilder->Finish();
  if (!expArray.ok()) {
    return tl::make_unexpected(expArray.status().message());
  }

  // compute the final aggregation
  const auto &expFinalResultScalar = arrow::compute::Sum(expArray.ValueOrDie());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  return (*expFinalResultScalar).scalar();
}

}
