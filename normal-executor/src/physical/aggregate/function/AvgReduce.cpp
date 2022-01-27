//
// Created by Yifei Yang on 1/25/22.
//

#include <normal/executor/physical/aggregate/function/AvgReduce.h>
#include <normal/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>
#include <arrow/compute/api_aggregate.h>

using namespace normal::plan::prephysical;

namespace normal::executor::physical::aggregate {

AvgReduce::AvgReduce(const string &outputColumnName,
                     const shared_ptr<normal::expression::gandiva::Expression> &expression)
  : AvgBase(AVG_REDUCE, outputColumnName, expression) {}

set<string> AvgReduce::involvedColumnNames() const {
  return {AggregatePrePFunction::AVG_PARALLEL_SUM_COLUMN_PREFIX + outputColumnName_,
          AggregatePrePFunction::AVG_PARALLEL_COUNT_COLUMN_PREFIX + outputColumnName_};
}

tl::expected<shared_ptr<AggregateResult>, string> AvgReduce::compute(const shared_ptr<TupleSet> &tupleSet) {
  // compute sum
  const auto &sumInputColumn = tupleSet->table()->GetColumnByName(
          AggregatePrePFunction::AVG_PARALLEL_SUM_COLUMN_PREFIX + outputColumnName_);
  if (!sumInputColumn) {
    return tl::make_unexpected(fmt::format("AVG_PARALLEL_SUM_COLUMN for {} not exist", outputColumnName_));
  }
  aggColumnDataType_ = sumInputColumn->type();
  const auto &expSumDatum = arrow::compute::Sum(sumInputColumn);
  if (!expSumDatum.ok()) {
    return tl::make_unexpected(expSumDatum.status().message());
  }
  const auto &sumScalar = (*expSumDatum).scalar();

  // compute count
  const auto &countInputColumn = tupleSet->table()->GetColumnByName(
          AggregatePrePFunction::AVG_PARALLEL_COUNT_COLUMN_PREFIX + outputColumnName_);
  if (!countInputColumn) {
    return tl::make_unexpected(fmt::format("AVG_PARALLEL_COUNT_COLUMN for {} not exist", outputColumnName_));
  }
  const auto &expCountDatum = arrow::compute::Sum(countInputColumn);
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
