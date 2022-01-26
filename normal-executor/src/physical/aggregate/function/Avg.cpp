//
// Created by Yifei Yang on 1/25/22.
//

#include <normal/executor/physical/aggregate/function/Avg.h>
#include <normal/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

namespace normal::executor::physical::aggregate {

Avg::Avg(const string &outputColumnName,
         const shared_ptr<normal::expression::gandiva::Expression> &expression)
  : AggregateFunction(AVG, outputColumnName, expression) {}

shared_ptr<arrow::DataType> Avg::returnType() const {
  return arrow::float64();
}

tl::expected<shared_ptr<AggregateResult>, string> Avg::compute(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }
  const auto &aggChunkedArray = *expAggChunkedArray;

  // compute sum
  const auto &expSumDatum = arrow::compute::Sum(aggChunkedArray);
  if (!expSumDatum.ok()) {
    return tl::make_unexpected(expSumDatum.status().message());
  }
  const auto &sumScalar = (*expSumDatum).scalar();

  // compute count
  const auto &expCountDatum = arrow::compute::Count(*expAggChunkedArray);
  if (!expCountDatum.ok()) {
    return tl::make_unexpected(expCountDatum.status().message());
  }
  const auto &countScalar = (*expCountDatum).scalar();

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(SUM_RESULT_KEY, sumScalar);
  aggregateResult->put(COUNT_RESULT_KEY, countScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Avg::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input arrays
  const auto expFinalizeSumInputArray = buildFinalizeInputArray(aggregateResults, SUM_RESULT_KEY, aggColumnDataType_);
  if (!expFinalizeSumInputArray) {
    return tl::make_unexpected(expFinalizeSumInputArray.error());
  }
  const auto expFinalizeCountInputArray = buildFinalizeInputArray(aggregateResults, COUNT_RESULT_KEY, arrow::int64());
  if (!expFinalizeCountInputArray) {
    return tl::make_unexpected(expFinalizeCountInputArray.error());
  }

  // compute final sum and count
  const auto &expFinalSumScalar = arrow::compute::Sum(expFinalizeSumInputArray.value());
  if (!expFinalSumScalar.ok()) {
    return tl::make_unexpected(expFinalSumScalar.status().message());
  }
  const auto &finalSumScalar = (*expFinalSumScalar).scalar();
  const auto &expFinalCountScalar = arrow::compute::Sum(expFinalizeCountInputArray.value());
  if (!expFinalCountScalar.ok()) {
    return tl::make_unexpected(expFinalCountScalar.status().message());
  }
  const auto &finalCountScalar = (*expFinalCountScalar).scalar();

  // compute average
  const auto &expAvgScalar = arrow::compute::CallFunction("divide", {finalSumScalar, finalCountScalar});
  if (!expAvgScalar.ok()) {
    return tl::make_unexpected(expAvgScalar.status().message());
  }
  const auto &avgScalar = (*expAvgScalar).scalar();

  // cast to float64 to avoid implicit cast at downstream
  const auto &expCastScalar = arrow::compute::Cast(avgScalar, arrow::float64());
  if (!expCastScalar.ok()) {
    return tl::make_unexpected(expCastScalar.status().message());
  }
  return (*expCastScalar).scalar();
}

}
