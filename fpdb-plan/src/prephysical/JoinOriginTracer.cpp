//
// Created by Yifei Yang on 4/14/23.
//

#include <fpdb/plan/prephysical/JoinOriginTracer.h>
#include <unordered_map>

namespace fpdb::plan::prephysical {

std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>
JoinOriginTracer::trace(const std::shared_ptr<PrePhysicalPlan> &plan) {
  JoinOriginTracer tracer(plan);
  tracer.trace();
  return tracer.mergeSingleJoinOrigins();
}

JoinOriginTracer::JoinOriginTracer(const std::shared_ptr<PrePhysicalPlan> &plan):
  plan_(plan) {}

void JoinOriginTracer::trace() {
  std::vector<std::shared_ptr<ColumnOrigin>> emptyColumnOrigins{};
  traceDFS(plan_->getRootOp(), emptyColumnOrigins);
}

void JoinOriginTracer::traceDFS(const std::shared_ptr<PrePhysicalOp> &op,
                                const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  switch (op->getType()) {
    case PrePOpType::AGGREGATE: {
      return traceAggregate(std::static_pointer_cast<AggregatePrePOp>(op), columnOrigins);
    }
    case PrePOpType::GROUP: {
      return traceGroup(std::static_pointer_cast<GroupPrePOp>(op), columnOrigins);
    }
    case PrePOpType::SORT: {
      return traceSort(std::static_pointer_cast<SortPrePOp>(op), columnOrigins);
    }
    case PrePOpType::LIMIT_SORT: {
      return traceLimitSort(std::static_pointer_cast<LimitSortPrePOp>(op), columnOrigins);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      return traceFilterableScan(std::static_pointer_cast<FilterableScanPrePOp>(op), columnOrigins);
    }
    case PrePOpType::FILTER: {
      return traceFilter(std::static_pointer_cast<FilterPrePOp>(op), columnOrigins);
    }
    case PrePOpType::PROJECT: {
      return traceProject(std::static_pointer_cast<ProjectPrePOp>(op), columnOrigins);
    }
    case PrePOpType::HASH_JOIN: {
      return traceHashJoin(std::static_pointer_cast<HashJoinPrePOp>(op), columnOrigins);
    }
    case PrePOpType::NESTED_LOOP_JOIN: {
      return traceNestedLoopJoin(std::static_pointer_cast<NestedLoopJoinPrePOp>(op), columnOrigins);
    }
    default: {
      throw std::runtime_error(
              fmt::format("Unsupported operator type to trace join origin: '{}'", op->getTypeString()));
    }
  }
}

void JoinOriginTracer::traceAggregate(const std::shared_ptr<AggregatePrePOp> &op,
                                      const std::vector<std::shared_ptr<ColumnOrigin>> &) {
  // aggregate op invalidates input column origins
  std::vector<std::shared_ptr<ColumnOrigin>> emptyColumnOrigins{};
  traceDFS(op->getProducers()[0], emptyColumnOrigins);
}

void JoinOriginTracer::traceGroup(const std::shared_ptr<GroupPrePOp> &op,
                                  const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // only columns in group keys are valid
  auto groupColumns = op->getGroupColumnNames();
  std::set<std::string> groupColumnSet(groupColumns.begin(), groupColumns.end());
  std::vector<std::shared_ptr<ColumnOrigin>> validColumnOrigins;
  for (const auto &columnOrigin: columnOrigins) {
    if (groupColumnSet.find(columnOrigin->currName_) != groupColumnSet.end()) {
      validColumnOrigins.emplace_back(columnOrigin);
    }
  }
  traceDFS(op->getProducers()[0], validColumnOrigins);
}

void JoinOriginTracer::traceSort(const std::shared_ptr<SortPrePOp> &op,
                                 const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for sort op
  traceDFS(op->getProducers()[0], columnOrigins);
}

void JoinOriginTracer::traceLimitSort(const std::shared_ptr<LimitSortPrePOp> &op,
                                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for limit sort op
  traceDFS(op->getProducers()[0], columnOrigins);
}


void JoinOriginTracer::traceFilter(const std::shared_ptr<FilterPrePOp> &op,
                                   const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for filter op
  traceDFS(op->getProducers()[0], columnOrigins);
}

void JoinOriginTracer::traceFilterableScan(const std::shared_ptr<FilterableScanPrePOp> &op,
                                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  const auto &scanColumns = op->getProjectColumnNames();
  std::set<std::string> scanColumnSet(scanColumns.begin(), scanColumns.end());
  for (auto &columnOrigin: columnOrigins) {
    if (scanColumnSet.find(columnOrigin->currName_) != scanColumnSet.end()) {
      columnOrigin->originOp_ = op;
    }
  }
}

void JoinOriginTracer::traceProject(const std::shared_ptr<ProjectPrePOp> &op,
                                    const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // get a map for 'new name' -> 'old name'
  std::unordered_map<std::string, std::string> newToOldName;
  for (const auto &rename: op->getProjectColumnNamePairs()) {
    newToOldName[rename.second] = rename.first;
  }

  // convert currName_ for input column origins
  for (const auto &columnOrigin: columnOrigins) {
    const auto &newToOldNameIt = newToOldName.find(columnOrigin->currName_);
    if (newToOldNameIt != newToOldName.end()) {
      columnOrigin->currName_ = newToOldNameIt->second;
    }
  }

  // trace the parent node
  traceDFS(op->getProducers()[0], columnOrigins);
}

void JoinOriginTracer::traceHashJoin(const std::shared_ptr<HashJoinPrePOp> &op,
                                     const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // construct left and right column origins
  std::vector<std::shared_ptr<ColumnOrigin>> leftColumnOrigins;
  std::vector<std::shared_ptr<ColumnOrigin>> rightColumnOrigins;
  for (const std::string &leftColumn: op->getLeftColumnNames()) {
    leftColumnOrigins.emplace_back(std::make_shared<ColumnOrigin>(leftColumn));
  }
  for (const std::string &rightColumn: op->getRightColumnNames()) {
    rightColumnOrigins.emplace_back(std::make_shared<ColumnOrigin>(rightColumn));
  }

  // we cannot distinguish which of input column origins are in left, which are in right, so need to add to both
  leftColumnOrigins.insert(leftColumnOrigins.end(), columnOrigins.begin(), columnOrigins.end());
  rightColumnOrigins.insert(rightColumnOrigins.end(), columnOrigins.begin(), columnOrigins.end());

  // trace parent nodes
  traceDFS(op->getProducers()[0], leftColumnOrigins);
  traceDFS(op->getProducers()[1], rightColumnOrigins);

  // construct join origins when trace is finished
  auto joinType = op->getJoinType();
  for (int i = 0; i < op->getNumJoinColumnPairs(); ++i) {
    const auto &leftColumnOriginOp = leftColumnOrigins[i]->originOp_;
    const auto &rightColumnOriginOp = rightColumnOrigins[i]->originOp_;
    const auto &leftColumn = leftColumnOrigins[i]->currName_;
    const auto &rightColumn = rightColumnOrigins[i]->currName_;
    if (leftColumnOriginOp != nullptr && rightColumnOriginOp != nullptr) {
      if (leftColumnOriginOp->getRowCount() <= rightColumnOriginOp->getRowCount()) {
        singleJoinOrigins_.emplace_back(std::make_shared<SingleJoinOrigin>(
                leftColumnOriginOp, rightColumnOriginOp, leftColumn, rightColumn, joinType));
      } else {
        auto expReversedJoinType = reverseJoinType(joinType);
        if (!expReversedJoinType.has_value()) {
          throw std::runtime_error(expReversedJoinType.error());
        }
        singleJoinOrigins_.emplace_back(std::make_shared<SingleJoinOrigin>(
                rightColumnOriginOp, leftColumnOriginOp, rightColumn, leftColumn, *expReversedJoinType));
      }
    }
  }
}

void JoinOriginTracer::traceNestedLoopJoin(const std::shared_ptr<NestedLoopJoinPrePOp> &op,
                                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // we cannot distinguish which of input column origins are in left, which are in right, so need to add to both
  traceDFS(op->getProducers()[0], columnOrigins);
  traceDFS(op->getProducers()[1], columnOrigins);
}

std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>
JoinOriginTracer::mergeSingleJoinOrigins() {
  std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> joinOrigins;
  for (const auto &singleJoinOrigin: singleJoinOrigins_) {
    auto newJoinOrigin = std::make_shared<JoinOrigin>(
            singleJoinOrigin->left_, singleJoinOrigin->right_, singleJoinOrigin->joinType_);
    auto joinOriginIt = joinOrigins.find(newJoinOrigin);
    if (joinOriginIt != joinOrigins.end()) {
      (*joinOriginIt)->addJoinColumnPair(singleJoinOrigin->leftColumn_, singleJoinOrigin->rightColumn_);
    } else {
      newJoinOrigin->addJoinColumnPair(singleJoinOrigin->leftColumn_, singleJoinOrigin->rightColumn_);
      joinOrigins.emplace(newJoinOrigin);
    }
  }
  return joinOrigins;
}

}
