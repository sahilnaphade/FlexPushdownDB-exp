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
        const std::shared_ptr<TupleSet> &buildTupleSet, const std::shared_ptr<TupleSet> &probeTupleSet) {
  // prepare schema and keys
  std::vector<arrow::FieldRef> buildJoinKeys, probeJoinKeys;
  for (const auto &leftColumn: pred_.getLeftColumnNames()) {
    buildJoinKeys.emplace_back(leftColumn);
  }
  for (const auto &rightColumn: pred_.getRightColumnNames()) {
    probeJoinKeys.emplace_back(rightColumn);
  }

  std::vector<arrow::FieldRef> buildOutputKeys, probeOutputKeys;
  arrow::FieldVector outputFields;
  for (const auto &field: buildTupleSet->schema()->fields()) {
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      buildOutputKeys.emplace_back(field->name());
      outputFields.emplace_back(field);
    }
  }
  for (const auto &field: probeTupleSet->schema()->fields()) {
    if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
      probeOutputKeys.emplace_back(field->name());
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

  // two dummy input nodes at the beginning
  auto expBuildInputNode = arrow::compute::DummyNode::Make(execPlan.get(), buildTupleSet->schema());
  if (!expBuildInputNode.ok()) {
    return tl::make_unexpected(expBuildInputNode.status().message());
  }
  auto buildInputNode = *expBuildInputNode;

  auto expProbeInputNode = arrow::compute::DummyNode::Make(execPlan.get(), probeTupleSet->schema());
  if (!expProbeInputNode.ok()) {
    return tl::make_unexpected(expProbeInputNode.status().message());
  }
  auto probeInputNode = *expProbeInputNode;

  // hash join node, note arrow's impl builds hashtable using right table and probes using left table
  arrow::compute::HashJoinNodeOptions hashJoinNodeOptions{
          arrow::compute::JoinType::INNER,
          probeJoinKeys, buildJoinKeys,
          probeOutputKeys, buildOutputKeys
  };
  auto expHashJoinNode = arrow::compute::MakeExecNode("hashjoin", execPlan.get(), {probeInputNode, buildInputNode},
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

  // supply input batches to build side
  auto reader = std::make_shared<arrow::TableBatchReader>(*buildTupleSet->table());
  auto recordBatchReadResult = reader->Next();
  reader->set_chunksize(DefaultChunkSize);
  if (!recordBatchReadResult.ok()) {
    return tl::make_unexpected(recordBatchReadResult.status().message());
  }
  auto recordBatch = *recordBatchReadResult;

  int numBuildInputBatches = 0;
  while (recordBatch) {
    ++numBuildInputBatches;
    arrow::compute::ExecBatch execBatch(*recordBatch);
    hashJoinNode->InputReceived(buildInputNode, execBatch);

    // next batch
    recordBatchReadResult = reader->Next();
    if (!recordBatchReadResult.ok())
      return tl::make_unexpected(recordBatchReadResult.status().message());
    recordBatch = *recordBatchReadResult;
  }
  hashJoinNode->InputFinished(buildInputNode, numBuildInputBatches);

  // supply batches to probe side
  reader = std::make_shared<arrow::TableBatchReader>(*probeTupleSet->table());
  recordBatchReadResult = reader->Next();
  reader->set_chunksize(DefaultChunkSize);
  if (!recordBatchReadResult.ok()) {
    return tl::make_unexpected(recordBatchReadResult.status().message());
  }
  recordBatch = *recordBatchReadResult;

  int numProbeInputBatches = 0;
  while (recordBatch) {
    ++numProbeInputBatches;
    arrow::compute::ExecBatch execBatch(*recordBatch);
    hashJoinNode->InputReceived(probeInputNode, execBatch);

    // next batch
    recordBatchReadResult = reader->Next();
    if (!recordBatchReadResult.ok())
      return tl::make_unexpected(recordBatchReadResult.status().message());
    recordBatch = *recordBatchReadResult;
  }
  hashJoinNode->InputFinished(probeInputNode, numProbeInputBatches);

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
