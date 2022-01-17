//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/transform/PrePToPTransformer.h>
#include <normal/executor/physical/transform/PrePToS3PTransformer.h>
#include <normal/executor/physical/sort/SortPOp.h>
#include <normal/executor/physical/limitsort/LimitSortPOp.h>
#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/aggregate/function/Sum.h>
#include <normal/executor/physical/aggregate/function/Count.h>
#include <normal/executor/physical/aggregate/function/MinMax.h>
#include <normal/executor/physical/group/GroupPOp.h>
#include <normal/executor/physical/project/ProjectPOp.h>
#include <normal/executor/physical/filter/FilterPOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <normal/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <normal/executor/physical/shuffle/ShufflePOp.h>
#include <normal/executor/physical/split/SplitPOp.h>
#include <normal/executor/physical/collate/CollatePOp.h>
#include <normal/expression/gandiva/Column.h>

namespace normal::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<AWSClient> &awsClient,
                                       const shared_ptr<Mode> &mode,
                                       int parallelDegree) :
  prePhysicalPlan_(prePhysicalPlan),
  awsClient_(awsClient),
  mode_(mode),
  parallelDegree_(parallelDegree) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // transform from root in dfs
  auto rootTransRes = transformDfs(prePhysicalPlan_->getRootOp());
  auto upConnPOps = rootTransRes.first;
  auto allPOps = rootTransRes.second;

  // make a collate operator
  shared_ptr<PhysicalOp> collatePOp = make_shared<collate::CollatePOp>(
          "collate", ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()));
  allPOps.emplace_back(collatePOp);
  connectManyToOne(upConnPOps, collatePOp);

  return make_shared<PhysicalPlan>(allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    case SORT: {
      const auto &sortPrePOp = std::static_pointer_cast<SortPrePOp>(prePOp);
      return transformSort(sortPrePOp);
    }
    case LIMIT_SORT: {
      const auto &limitSortPrePOp = std::static_pointer_cast<LimitSortPrePOp>(prePOp);
      return transformLimitSort(limitSortPrePOp);
    }
    case AGGREGATE: {
      const auto &aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(prePOp);
      return transformAggregate(aggregatePrePOp);
    }
    case GROUP: {
      const auto &groupPrePOp = std::static_pointer_cast<GroupPrePOp>(prePOp);
      return transformGroup(groupPrePOp);
    }
    case PROJECT: {
      const auto &projectPrePOp = std::static_pointer_cast<ProjectPrePOp>(prePOp);
      return transformProject(projectPrePOp);
    }
    case FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case HASH_JOIN: {
      const auto &hashJoinPrePOp = std::static_pointer_cast<HashJoinPrePOp>(prePOp);
      return transformHashJoin(hashJoinPrePOp);
    }
    case NESTED_LOOP_JOIN: {
      const auto &nestedLoopJoinPrePOp = std::static_pointer_cast<NestedLoopJoinPrePOp>(prePOp);
      return transformNestedLoopJoin(nestedLoopJoinPrePOp);
    }
    case FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
PrePToPTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformSort(const shared_ptr<SortPrePOp> &sortPrePOp) {
  // id
  auto prePOpId = sortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(sortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for sort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> sortPOp = make_shared<sort::SortPOp>(fmt::format("Sort[{}]", prePOpId),
                                                              sortPrePOp->getSortOptions(),
                                                              projectColumnNames);
  allPOps.emplace_back(sortPOp);

  // connect to upstream
  connectManyToOne(upConnPOps, sortPOp);

  return make_pair(vector<shared_ptr<PhysicalOp>>{sortPOp}, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp) {
  // id
  auto prePOpId = limitSortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(limitSortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for limitSort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<string> projectColumnNames{limitSortPrePOp->getProjectColumnNames().begin(),
                                    limitSortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> limitSortPOp = make_shared<limitsort::LimitSortPOp>(
          fmt::format("LimitSort[{}]", prePOpId),
          limitSortPrePOp->getSelectKOptions(),
          projectColumnNames);
  allPOps.emplace_back(limitSortPOp);

  // connect to upstream
  connectManyToOne(upConnPOps, limitSortPOp);

  return make_pair(vector<shared_ptr<PhysicalOp>>{limitSortPOp}, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // id
  auto prePOpId = aggregatePrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for aggregate, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prepFunction = aggregatePrePOp->getFunctions()[j];
      const auto &alias = aggregatePrePOp->getAggOutputColumnNames()[j];
      aggFunctions.emplace_back(transformAggFunction(alias, prepFunction));
    }

    selfPOps.emplace_back(make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-{}", prePOpId, i),
            aggFunctions,
            projectColumnNames));
  }

  // if num > 1, then we need a aggregate reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = selfPOps;
    selfConnDownPOps = selfPOps;
  } else {
    // aggregate reduce functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prepFunction = aggregatePrePOp->getFunctions()[j];
      const auto &alias = aggregatePrePOp->getAggOutputColumnNames()[j];
      aggReduceFunctions.emplace_back(transformAggReduceFunction(alias, prepFunction));
    }

    shared_ptr<PhysicalOp> aggReducePOp = make_shared<aggregate::AggregatePOp>(
            fmt::format("AggregateReduce[{}]", prePOpId),
            aggReduceFunctions,
            projectColumnNames);
    connectManyToOne(selfPOps, aggReducePOp);
    selfConnUpPOps = selfPOps;
    selfConnDownPOps.emplace_back(aggReducePOp);
    selfPOps.emplace_back(aggReducePOp);
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfConnUpPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfConnDownPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                    groupPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prepFunction = groupPrePOp->getFunctions()[j];
      const auto &alias = groupPrePOp->getAggOutputColumnNames()[j];
      aggFunctions.emplace_back(transformAggFunction(alias, prepFunction));
    }

    selfPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}", prePOpId, i),
                                                       groupPrePOp->getGroupColumnNames(),
                                                       aggFunctions,
                                                       projectColumnNames));
  }

  // if num > 1, then we need a group reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = selfPOps;
    selfConnDownPOps = selfPOps;
  } else {
    // aggregate reduce functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prepFunction = groupPrePOp->getFunctions()[j];
      const auto &alias = groupPrePOp->getAggOutputColumnNames()[j];
      aggReduceFunctions.emplace_back(transformAggReduceFunction(alias, prepFunction));
    }

    shared_ptr<PhysicalOp> groupReducePOp = make_shared<group::GroupPOp>(fmt::format("GroupReduce[{}]", prePOpId),
                                                                         groupPrePOp->getGroupColumnNames(),
                                                                         aggReduceFunctions,
                                                                         projectColumnNames);
    connectManyToOne(selfPOps, groupReducePOp);
    selfConnUpPOps = selfPOps;
    selfConnDownPOps.emplace_back(groupReducePOp);
    selfPOps.emplace_back(groupReducePOp);
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfConnUpPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfConnDownPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp) {
  // id
  auto prePOpId = projectPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(projectPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for project, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{projectPrePOp->getProjectColumnNames().begin(),
                                    projectPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<project::ProjectPOp>(fmt::format("Project[{}]-{}", prePOpId, i),
                                                           projectPrePOp->getExprs(),
                                                           projectPrePOp->getExprNames(),
                                                           projectPrePOp->getProjectColumnNamePairs(),
                                                           projectColumnNames));
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {
  // id
  auto prePOpId = filterPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(filterPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for filter, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{filterPrePOp->getProjectColumnNames().begin(),
                                    filterPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}", prePOpId, i),
                                                         filterPrePOp->getPredicate(),
                                                         nullptr,
                                                         projectColumnNames));
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // id
  auto prePOpId = hashJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(hashJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for hashJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }

  auto leftTransRes = producersTransRes[0];
  auto rightTransRes = producersTransRes[1];
  auto allPOps = leftTransRes.second;
  allPOps.insert(allPOps.end(), rightTransRes.second.begin(), rightTransRes.second.end());

  // transform self
  vector<string> projectColumnNames{hashJoinPrePOp->getProjectColumnNames().begin(),
                                    hashJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = hashJoinPrePOp->getJoinType();
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();
  join::HashJoinPredicate hashJoinPredicate(leftColumnNames, rightColumnNames);
  const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
  for (int i = 0; i < parallelDegree_; ++i) {
    hashJoinBuildPOps.emplace_back(make_shared<join::HashJoinBuildPOp>(
            fmt::format("HashJoinBuild[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
            leftColumnNames,
            projectColumnNames));
    hashJoinProbePOps.emplace_back(make_shared<join::HashJoinProbePOp>(
            fmt::format("HashJoinProbe[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
            hashJoinPredicate,
            joinType,
            projectColumnNames));
  }
  allPOps.insert(allPOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
  allPOps.insert(allPOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());
  connectOneToOne(hashJoinBuildPOps, hashJoinProbePOps);

  // if num > 1, then we need shuffle operators
  if (parallelDegree_ == 1) {
    // connect to upstream
    connectManyToOne(leftTransRes.first, hashJoinBuildPOps[0]);
    connectManyToOne(rightTransRes.first, hashJoinProbePOps[0]);
  } else {
    vector<shared_ptr<PhysicalOp>> shuffleLeftPOps, shuffleRightPOps;
    for (const auto &upLeftConnPOp: leftTransRes.first) {
      shared_ptr<shuffle::ShufflePOp> shufflePOp = make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upLeftConnPOp->name()),
              leftColumnNames,
              upLeftConnPOp->getProjectColumnNames());
      shuffleLeftPOps.emplace_back(shufflePOp);

      // ShufflePOp's produce() overrides the base one, so we cannot use connectManyToMany() here
      for (const auto &hashJoinBuildPOp: hashJoinBuildPOps) {
        shufflePOp->produce(hashJoinBuildPOp);
        hashJoinBuildPOp->consume(shufflePOp);
      }
    }
    for (const auto &upRightConnPOp: rightTransRes.first) {
      shared_ptr<shuffle::ShufflePOp> shufflePOp = make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upRightConnPOp->name()),
              rightColumnNames,
              upRightConnPOp->getProjectColumnNames());
      shuffleRightPOps.emplace_back(shufflePOp);

      // ShufflePOp's produce() overrides the base one, so we cannot use connectManyToMany() here
      for (const auto &hashJoinProbePOp: hashJoinProbePOps) {
        shufflePOp->produce(hashJoinProbePOp);
        hashJoinProbePOp->consume(shufflePOp);
      }
    }
    allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());

    // connect to upstream
    connectOneToOne(leftTransRes.first, shuffleLeftPOps);
    connectOneToOne(rightTransRes.first, shuffleRightPOps);
  };

  return make_pair(hashJoinProbePOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp) {
  // id
  auto prePOpId = nestedLoopJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(nestedLoopJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for nestedLoopJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }

  auto leftTransRes = producersTransRes[0];
  auto rightTransRes = producersTransRes[1];
  auto allPOps = leftTransRes.second;
  allPOps.insert(allPOps.end(), rightTransRes.second.begin(), rightTransRes.second.end());

  // transform self
  vector<string> projectColumnNames{nestedLoopJoinPrePOp->getProjectColumnNames().begin(),
                                    nestedLoopJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = nestedLoopJoinPrePOp->getJoinType();
  optional<shared_ptr<normal::expression::gandiva::Expression>> predicate;
  if (nestedLoopJoinPrePOp->getPredicate()) {
    predicate = nestedLoopJoinPrePOp->getPredicate();
  }

  vector<shared_ptr<PhysicalOp>> nestedLoopJoinPOps;
  nestedLoopJoinPOps.reserve(parallelDegree_);
  for (int i = 0; i < parallelDegree_; ++i) {
    shared_ptr<join::NestedLoopJoinPOp> nestedLoopJoinPOp =
            make_shared<join::NestedLoopJoinPOp>(fmt::format("NestedLoopJoin[{}]-{}", prePOpId, i),
                                                 predicate,
                                                 joinType,
                                                 projectColumnNames);
    // connect to left inputs
    for (const auto &upLeftConnPOp: leftTransRes.first) {
      upLeftConnPOp->produce(nestedLoopJoinPOp);
      nestedLoopJoinPOp->addLeftProducer(upLeftConnPOp);
    }
    nestedLoopJoinPOps.emplace_back(nestedLoopJoinPOp);
  }
  allPOps.insert(allPOps.end(), nestedLoopJoinPOps.begin(), nestedLoopJoinPOps.end());

  // connect to right inputs, if num > 1, then we need split operators for right producers
  if (parallelDegree_ == 1) {
    for (const auto &upRightConnPOp: rightTransRes.first) {
      upRightConnPOp->produce(nestedLoopJoinPOps[0]);
      static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOps[0])->addRightProducer(upRightConnPOp);
    }
  } else {
    vector<shared_ptr<PhysicalOp>> splitPOps;
    for (const auto &upRightConnPOp: rightTransRes.first) {
      shared_ptr<split::SplitPOp> splitPOp = make_shared<split::SplitPOp>(
              fmt::format("Split[{}]-{}", prePOpId, upRightConnPOp->name()),
              upRightConnPOp->getProjectColumnNames());
      splitPOps.emplace_back(splitPOp);

      // SplitPOp's produce() overrides the base one, so we cannot use connectManyToMany() here
      for (const auto &nestedLoopJoinPOp: nestedLoopJoinPOps) {
        splitPOp->produce(nestedLoopJoinPOp);
        static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOp)->addRightProducer(splitPOp);
      }
    }
    allPOps.insert(allPOps.end(), splitPOps.begin(), splitPOps.end());

    // connect to upstream
    connectOneToOne(rightTransRes.first, splitPOps);
  }

  return make_pair(nestedLoopJoinPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto s3PTransformer = make_shared<PrePToS3PTransformer>(filterableScanPrePOp->getId(),
                                                                awsClient_,
                                                                mode_);
  return s3PTransformer->transformFilterableScan(filterableScanPrePOp);
}

shared_ptr<aggregate::AggregateFunction>
PrePToPTransformer::transformAggFunction(const string &outputColumnName,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(outputColumnName, prePFunction->getExpression());
    }
    case plan::prephysical::COUNT: {
      return make_shared<aggregate::Count>(outputColumnName, prePFunction->getExpression());
    }
    case plan::prephysical::MIN: {
      return make_shared<aggregate::MinMax>(true, outputColumnName, prePFunction->getExpression());
    }
    case plan::prephysical::MAX: {
      return make_shared<aggregate::MinMax>(false, outputColumnName, prePFunction->getExpression());
    }
    case plan::prephysical::AVG:
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregateFunction>
PrePToPTransformer::transformAggReduceFunction(const string &outputColumnName,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(outputColumnName, normal::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::COUNT: {
      return make_shared<aggregate::Count>(outputColumnName, normal::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MIN: {
      return make_shared<aggregate::MinMax>(true, outputColumnName, normal::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MAX: {
      return make_shared<aggregate::MinMax>(false, outputColumnName, normal::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::AVG:
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

void PrePToPTransformer::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                         vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw runtime_error(fmt::format("Bad one-to-one operator connection input, producers has {}, but consumers has {}",
                                    producers.size(), consumers.size()));
  }
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

void PrePToPTransformer::connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                                           vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &producer: producers) {
    for (const auto &consumer: consumers) {
      producer->produce(consumer);
      consumer->consume(producer);
    }
  }
}

void PrePToPTransformer::connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                          shared_ptr<PhysicalOp> &consumer) {
  for (const auto &producer: producers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}

}
