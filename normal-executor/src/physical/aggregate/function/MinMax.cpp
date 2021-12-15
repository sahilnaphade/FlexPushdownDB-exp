//
// Created by Yifei Yang on 12/14/21.
//

#include <normal/executor/physical/aggregate/function/MinMax.h>
#include <arrow/compute/api_aggregate.h>

namespace normal::executor::physical::aggregate {

MinMax::MinMax(bool isMin,
               const string &outputColumnName,
               const shared_ptr<normal::expression::gandiva::Expression> &expression):
  AggregateFunction(outputColumnName, expression),
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
  const auto &aggChunkedArray = evaluateExpr(tupleSet);

  // compute the aggregation
  const auto &expResultDatum = arrow::compute::MinMax(aggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  // TODO
  const auto &resultScalar = (*expResultDatum).scalar();

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  auto key = isMin_ ? MIN_RESULT_KEY : MAX_RESULT_KEY;
  aggregateResult->put(key, resultScalar);
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
  // TODO
  const auto &expFinalResultScalar = arrow::compute::MinMax(expFinalizeInputArray.value());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  return (*expFinalResultScalar).scalar();
}

}
