//
// Created by Yifei Yang on 4/25/22.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowKernel.h>
#include <fpdb/tuple/util/DummyNode.h>
#include <arrow/compute/exec/exec_plan.h>

namespace fpdb::executor::physical::join {

HashJoinArrowKernel::HashJoinArrowKernel(const HashJoinPredicate &pred,
                                         const std::set<std::string> &neededColumnNames,
                                         JoinType joinType):
  pred_(pred),
  neededColumnNames_(neededColumnNames),
  joinType_(joinType) {}

std::shared_ptr<HashJoinArrowKernel> HashJoinArrowKernel::make(const HashJoinPredicate &pred,
                                                               const std::set<std::string> &neededColumnNames,
                                                               JoinType joinType) {
  return std::make_shared<HashJoinArrowKernel>(pred, neededColumnNames, joinType);
}

tl::expected<std::shared_ptr<TupleSet>, std::string> HashJoinArrowKernel::join(
        const std::shared_ptr<TupleSet> &leftTupleSet, const std::shared_ptr<TupleSet> &rightTupleSet) {
  // prepare schema and keys
  std::vector<arrow::FieldRef> leftJoinKeys, rightJoinKeys;
  for (const auto &leftColumn: pred_.getLeftColumnNames()) {
    leftJoinKeys.emplace_back(leftColumn);
  }
  for (const auto &rightColumn: pred_.getRightColumnNames()) {
    rightJoinKeys.emplace_back(rightColumn);
  }

  std::vector<arrow::FieldRef> leftOutputKeys, rightOutputKeys;
  arrow::FieldVector outputFields;
  for (const auto &field: leftTupleSet->schema()->fields()) {
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      leftOutputKeys.emplace_back(field->name());
      outputFields.emplace_back(field);
    }
  }
  for (const auto &field: rightTupleSet->schema()->fields()) {
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      rightOutputKeys.emplace_back(field->name());
      outputFields.emplace_back(field);
    }
  }
  auto outputSchema = arrow::schema(outputFields);

  // make arrow exec plan
  auto execContext = std::make_shared<arrow::compute::ExecContext>(arrow::default_memory_pool(),
                                                                   arrow::internal::GetCpuThreadPool());
  auto expExecPlan = arrow::compute::ExecPlan::Make(execContext.get());
  if (!expExecPlan.ok()) {
    return tl::make_unexpected(expExecPlan.status().message());
  }
  auto execPlan = *expExecPlan;
  execPlan->exec_context()->set_use_threads(false);

  // two dummy nodes at the beginning
  auto expLeftDummyNode = arrow::compute::DummyNode::Make(execPlan.get(), leftTupleSet->schema());
  if (!expLeftDummyNode.ok()) {
    return tl::make_unexpected(expLeftDummyNode.status().message());
  }
  auto leftDummyNode = *expLeftDummyNode;

  auto expRightDummyNode = arrow::compute::DummyNode::Make(execPlan.get(), rightTupleSet->schema());
  if (!expRightDummyNode.ok()) {
    return tl::make_unexpected(expRightDummyNode.status().message());
  }
  auto rightDummyNode = *expRightDummyNode;

  // hash join node
  arrow::compute::HashJoinNodeOptions hashJoinNodeOptions{
          arrow::compute::JoinType::INNER,
          leftJoinKeys, rightJoinKeys,
          leftOutputKeys, rightOutputKeys,
          "l_", "r_"
  };
  auto expHashJoinNode = arrow::compute::MakeExecNode("hashjoin", execPlan.get(), {leftDummyNode, rightDummyNode},
                                                       hashJoinNodeOptions);
  if (!expHashJoinNode.ok()) {
    return tl::make_unexpected(expHashJoinNode.status().message());
  }
  auto hashJoinNode = *expHashJoinNode;

  // sink node at the end
  auto sinkGen = std::make_shared<arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>>>();
  auto expSinkNode = arrow::compute::MakeExecNode("sink", execPlan.get(), {hashJoinNode},
                                                  arrow::compute::SinkNodeOptions{sinkGen.get()});
  if (!expSinkNode.ok()) {
    return tl::make_unexpected(expSinkNode.status().message());
  }
  auto sinkNode = *expSinkNode;

  // start
  auto status = hashJoinNode->StartProducing();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  status = sinkNode->StartProducing();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // supply left input batches
  auto reader = std::make_shared<arrow::TableBatchReader>(*leftTupleSet->table());
  auto recordBatchReadResult = reader->Next();
  reader->set_chunksize(DefaultChunkSize);
  if (!recordBatchReadResult.ok()) {
    return tl::make_unexpected(recordBatchReadResult.status().message());
  }
  auto recordBatch = *recordBatchReadResult;

  int numLeftInputBatches = 0;
  while (recordBatch) {
    ++numLeftInputBatches;
    arrow::compute::ExecBatch execBatch(*recordBatch);
    hashJoinNode->InputReceived(leftDummyNode, execBatch);

    // next batch
    recordBatchReadResult = reader->Next();
    if (!recordBatchReadResult.ok())
      return tl::make_unexpected(recordBatchReadResult.status().message());
    recordBatch = *recordBatchReadResult;
  }
  hashJoinNode->InputFinished(leftDummyNode, numLeftInputBatches);

  // supply right input batches
  reader = std::make_shared<arrow::TableBatchReader>(*rightTupleSet->table());
  recordBatchReadResult = reader->Next();
  reader->set_chunksize(DefaultChunkSize);
  if (!recordBatchReadResult.ok()) {
    return tl::make_unexpected(recordBatchReadResult.status().message());
  }
  recordBatch = *recordBatchReadResult;

  int numRightInputBatches = 0;
  while (recordBatch) {
    ++numRightInputBatches;
    arrow::compute::ExecBatch execBatch(*recordBatch);
    hashJoinNode->InputReceived(rightDummyNode, execBatch);

    // next batch
    recordBatchReadResult = reader->Next();
    if (!recordBatchReadResult.ok())
      return tl::make_unexpected(recordBatchReadResult.status().message());
    recordBatch = *recordBatchReadResult;
  }
  hashJoinNode->InputFinished(rightDummyNode, numRightInputBatches);

  // collect result table from sink node
  auto sinkReader = arrow::compute::MakeGeneratorReader(outputSchema,
                                                        *sinkGen,
                                                        execPlan->exec_context()->memory_pool());
  auto expOutputTable = arrow::Table::FromRecordBatchReader(sinkReader.get());
  if (!expOutputTable.ok()) {
    return tl::make_unexpected(expOutputTable.status().message());
  }

  return TupleSet::make(*expOutputTable);
}

}
