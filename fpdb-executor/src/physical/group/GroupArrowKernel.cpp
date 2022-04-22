//
// Created by Yifei Yang on 4/20/22.
//

#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/util/async_generator.h>

namespace fpdb::executor::physical::group {

GroupArrowKernel::GroupArrowKernel(const std::vector<std::string> &groupColumnNames,
                                   const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions):
  GroupAbstractKernel(GroupKernelType::GROUP_ARROW_KERNEL, groupColumnNames, aggregateFunctions) {}

tl::expected<void, std::string> GroupArrowKernel::group(const std::shared_ptr<TupleSet> &tupleSet) {
  // evaluate expr
  auto expEvaluatedTupleSet = evaluateExpr(tupleSet);
  if (!expEvaluatedTupleSet.has_value()) {
    return tl::make_unexpected(expEvaluatedTupleSet.error());
  }

  // prepare for group operation, i.e. make outputSchema and aggregateNodeOptions
  prepareGroup(tupleSet);

  // group evaluated
  auto expGroupedTupleSet = doGroup(*expEvaluatedTupleSet);
  if (!expGroupedTupleSet.has_value()) {
    return tl::make_unexpected(expGroupedTupleSet.error());
  }

  // buffer group result
  groupResults_.emplace_back(*expGroupedTupleSet);

  return {};
}

tl::expected<shared_ptr<TupleSet>, std::string> GroupArrowKernel::finalise() {
  if (groupResults_.empty()) {
    // TODO
    return tl::make_unexpected("No input tupleSet received");
  }

  else if (groupResults_.size() == 1) {
    return groupResults_[0];
  }

  else {
    // TODO
    return tl::make_unexpected("Group more than one input tupleSet is not supported");
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
GroupArrowKernel::evaluateExpr(const std::shared_ptr<TupleSet> &tupleSet) {
  arrow::FieldVector outputFields;
  arrow::ChunkedArrayVector outputColumns;

  // group columns
  for (const auto &groupColumnName: groupColumnNames_) {
    auto groupField = tupleSet->schema()->GetFieldByName(groupColumnName);
    if (groupField == nullptr) {
      return tl::make_unexpected(fmt::format("Group field '{}' not found in input table", groupColumnName));
    }
    outputFields.emplace_back(groupField);

    auto groupColumn = tupleSet->table()->GetColumnByName(groupColumnName);
    if (groupColumn == nullptr) {
      return tl::make_unexpected(fmt::format("Group column '{}' not found in input table", groupColumnName));
    }
    outputColumns.emplace_back(groupColumn);
  }

  // aggregate columns
  for (uint i = 0; i < aggregateFunctions_.size(); ++i) {
    auto function = aggregateFunctions_[i];

    auto expOutputColumn = function->evaluateExpr(tupleSet);
    if (!expOutputColumn.has_value()) {
      return tl::make_unexpected(expOutputColumn.error());
    }
    auto outputColumn = *expOutputColumn;

    outputColumns.emplace_back(outputColumn);
    outputFields.emplace_back(arrow::field(getAggregateColumnName(i), outputColumn->type()));
  }

  return TupleSet::make(arrow::schema(outputFields), outputColumns);
}

tl::expected<void, std::string> GroupArrowKernel::prepareGroup(const std::shared_ptr<TupleSet> &tupleSet) {
  // output schema
  arrow::FieldVector outputFields;
  for (const auto &function: aggregateFunctions_) {
    outputFields.emplace_back(arrow::field(function->getOutputColumnName(), function->returnType()));
  }
  for (const auto &groupColumnName: groupColumnNames_) {
    auto groupField = tupleSet->schema()->GetFieldByName(groupColumnName);
    if (groupField == nullptr) {
      return tl::make_unexpected(fmt::format("Group field '{}' not found in input schema", groupColumnName));
    }
    outputFields.emplace_back(groupField);
  }
  outputSchema_ = arrow::schema(outputFields);

  // aggregateNodeOptions
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  std::vector<arrow::compute::internal::Aggregate> aggregates;
  std::vector<arrow::FieldRef> targets;
  std::vector<std::string> names;
  std::vector<arrow::FieldRef> keys;

  for (uint i = 0; i < aggregateFunctions_.size(); ++i) {
    auto function = aggregateFunctions_[i];

    switch (function->getType()) {
      case AggregateFunctionType::SUM: {
        aggregates.emplace_back(arrow::compute::internal::Aggregate{"hash_sum", &defaultScalarAggregateOptions});
        names.emplace_back(function->getOutputColumnName());
        targets.emplace_back(getAggregateColumnName(i));
        break;
      }
      default: {
        return tl::make_unexpected(fmt::format("Aggregate function type not implemented for GroupArrowKernel: '{}'",
                                               function->getTypeString()));
      }
    }
  }

  for (const auto &groupColumnName: groupColumnNames_) {
    keys.emplace_back(arrow::FieldRef(groupColumnName));
  }

  aggregateNodeOptions_ = arrow::compute::AggregateNodeOptions(aggregates, targets, names, keys);

  return {};
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
GroupArrowKernel::doGroup(const std::shared_ptr<TupleSet> &tupleSet) {
  // initialize
  arrow::compute::ExecContext execContext(arrow::default_memory_pool(), arrow::internal::GetCpuThreadPool());

  // exec plan
  auto expExecPlan = arrow::compute::ExecPlan::Make(&execContext);
  if (!expExecPlan.ok()) {
    return tl::make_unexpected(expExecPlan.status().message());
  }
  auto execPlan = *expExecPlan;
  execPlan->exec_context()->set_use_threads(false);

  // source node
  auto reader = std::make_shared<arrow::TableBatchReader>(*tupleSet->table());
  auto expBatchGen = arrow::compute::MakeReaderGenerator(std::move(reader), execPlan->exec_context()->executor());
  if (!expBatchGen.ok()) {
    return tl::make_unexpected(expBatchGen.status().message());
  }
  arrow::compute::SourceNodeOptions sourceNodeOptions(tupleSet->schema(), *expBatchGen);
  auto expSourceNode = arrow::compute::MakeExecNode("source", execPlan.get(), {}, sourceNodeOptions);
  if (!expSourceNode.ok()) {
    return tl::make_unexpected(expSourceNode.status().message());
  }

  // hash-aggregate node
  auto expAggregateNode = arrow::compute::MakeExecNode("aggregate", execPlan.get(), {*expSourceNode},
                                                       *aggregateNodeOptions_);
  if (!expAggregateNode.ok()) {
    return tl::make_unexpected(expAggregateNode.status().message());
  }

  // sink node
  arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sinkGen;
  auto expSinkNode = arrow::compute::MakeExecNode("sink", execPlan.get(), {*expAggregateNode},
                                                  arrow::compute::SinkNodeOptions{&sinkGen});
  if (!expSinkNode.ok()) {
    return tl::make_unexpected(expSinkNode.status().message());
  }

  // start executing the plan
  auto status = execPlan->Validate();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  status = execPlan->StartProducing();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // collect result table from sink node
  auto sinkReader = arrow::compute::MakeGeneratorReader(*outputSchema_, std::move(sinkGen),
                                                        execPlan->exec_context()->memory_pool());
  auto expOutputTable = arrow::Table::FromRecordBatchReader(sinkReader.get());
  if (!expOutputTable.ok()) {
    return tl::make_unexpected(expOutputTable.status().message());
  }

  // stop the plan
  execPlan->StopProducing();
  auto future = execPlan->finished();
  if (!future.status().ok()) {
    return tl::make_unexpected(future.status().message());
  }

  // return result tupleSet
  return TupleSet::make(*expOutputTable);
}

std::string GroupArrowKernel::getAggregateColumnName(int functionId) {
  return AGGREGATE_COLUMN_PREFIX.data() + std::to_string(functionId);
}

void GroupArrowKernel::clear() {
  groupResults_.clear();
}

}
