//
// Created by Yifei Yang on 12/3/21.
//

#include <normal/executor/physical/aggregate/function/Count.h>
#include <arrow/compute/api_aggregate.h>

namespace normal::executor::physical::aggregate {

Count::Count(const string &outputColumnName,
             const shared_ptr<normal::expression::gandiva::Expression> &expression):
  AggregateFunction(outputColumnName, expression) {}

shared_ptr<arrow::DataType> Count::returnType() {
  return arrow::int64();
}

tl::expected<shared_ptr<AggregateResult>, string> Count::compute(const shared_ptr<TupleSet> &tupleSet) {
  // build aggregate input array
  shared_ptr<arrow::ChunkedArray> aggChunkedArray;
  if (expression_) {
    // if has expr, then evaluate it
    aggChunkedArray = evaluateExpr(tupleSet);
  } else {
    // otherwise, just use the first column
    aggChunkedArray = tupleSet->table()->column(0);
  }

  // compute the aggregation
  const auto &expResultScalar = arrow::compute::Count(aggChunkedArray);
  if (!expResultScalar.ok()) {
    return tl::make_unexpected(expResultScalar.status().message());
  }
  const auto &resultScalar = *expResultScalar;

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(COUNT_RESULT_KEY, resultScalar.scalar());
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Count::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, COUNT_RESULT_KEY);
  if (!expFinalizeInputArray) {
    return tl::make_unexpected(expFinalizeInputArray.error());
  }

  // compute the final aggregation
  const auto &expFinalResultScalar = arrow::compute::Sum(expFinalizeInputArray.value());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  return (*expFinalResultScalar).scalar();
}

}
