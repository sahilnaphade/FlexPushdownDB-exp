//
// Created by Yifei Yang on 12/2/21.
//

#include <normal/executor/physical/aggregate/function/Sum.h>
#include <arrow/compute/api_aggregate.h>

namespace normal::executor::physical::aggregate {

Sum::Sum(const string &outputColumnName,
         const shared_ptr<normal::expression::gandiva::Expression> &expression)
  : AggregateFunction(outputColumnName, expression) {}

shared_ptr<arrow::DataType> Sum::returnType() {
  return returnType_;
}

tl::expected<shared_ptr<AggregateResult>, string> Sum::compute(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &aggChunkedArray = evaluateExpr(tupleSet);

  // compute the aggregation
  const auto &expResultDatum = arrow::compute::Sum(aggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  const auto &resultScalar = (*expResultDatum).scalar();

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(SUM_RESULT_KEY, resultScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Sum::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, SUM_RESULT_KEY);
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