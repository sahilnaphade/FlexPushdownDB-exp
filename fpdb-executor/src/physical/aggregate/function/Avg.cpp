//
// Created by Yifei Yang on 1/25/22.
//

#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>

namespace fpdb::executor::physical::aggregate {

Avg::Avg(const string &outputColumnName,
         const shared_ptr<fpdb::expression::gandiva::Expression> &expression)
  : AvgBase(AVG, outputColumnName, expression) {}

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

}
