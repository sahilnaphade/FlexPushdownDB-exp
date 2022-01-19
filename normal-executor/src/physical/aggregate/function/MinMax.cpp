//
// Created by Yifei Yang on 12/14/21.
//

#include <normal/executor/physical/aggregate/function/MinMax.h>
#include <normal/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>

namespace normal::executor::physical::aggregate {

MinMax::MinMax(bool isMin,
               const string &outputColumnName,
               const shared_ptr<normal::expression::gandiva::Expression> &expression):
  AggregateFunction(MIN_MAX, outputColumnName, expression),
  isMin_(isMin) {}


shared_ptr<arrow::DataType> MinMax::returnType() {
  return returnType_;
}

set<string> MinMax::involvedColumnNames() {
  if (expression_) {
    return expression_->involvedColumnNames();
  } else {
    return set<string>();
  }
}

tl::expected<shared_ptr<AggregateResult>, string> MinMax::compute(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }

  // compute the aggregation
  const auto &expResultDatum = arrow::compute::MinMax(*expAggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  const auto &resultScalar = (*expResultDatum).scalar();

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  auto key = isMin_ ? MIN_RESULT_KEY : MAX_RESULT_KEY;
  const auto &structScalar = static_pointer_cast<arrow::StructScalar>(resultScalar);
  const shared_ptr<arrow::Scalar> minMaxScalar = isMin_ ? structScalar->value[0] : structScalar->value[1];
  aggregateResult->put(key, minMaxScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
MinMax::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  auto key = isMin_ ? MIN_RESULT_KEY : MAX_RESULT_KEY;
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, key);
  if (!expFinalizeInputArray) {
    return tl::make_unexpected(expFinalizeInputArray.error());
  }

  // compute the final aggregation
  const auto &expFinalResultScalar = arrow::compute::MinMax(expFinalizeInputArray.value());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  const auto &resultScalar = (*expFinalResultScalar).scalar();
  const auto &structScalar = static_pointer_cast<arrow::StructScalar>(resultScalar);
  const shared_ptr<arrow::Scalar> minMaxScalar = isMin_ ? structScalar->value[0] : structScalar->value[1];
  return minMaxScalar;
}

}
