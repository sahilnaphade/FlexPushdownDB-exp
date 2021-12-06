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
  shared_ptr<arrow::Scalar> resultScalar;

  if (expression_) {
    // if has expr, then evaluate it and call arrow api to count
    const auto &aggChunkedArray = evaluateExpr(tupleSet);
    const auto &expResultDatum = arrow::compute::Count(aggChunkedArray);
    if (!expResultDatum.ok()) {
      return tl::make_unexpected(expResultDatum.status().message());
    }
    resultScalar = (*expResultDatum).scalar();
  }

  else {
    // otherwise, it means count(*), so we should just return the number of rows
    const auto &expResultScalar = arrow::MakeScalar(returnType(), tupleSet->numRows());
    if (!expResultScalar.ok()) {
      return tl::make_unexpected(expResultScalar.status().message());
    }
    resultScalar = *expResultScalar;
  }

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(COUNT_RESULT_KEY, resultScalar);
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