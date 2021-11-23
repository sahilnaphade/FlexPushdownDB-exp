//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/transform/PrePToPTransformer.h>
#include <normal/executor/physical/transform/PrePToS3PTransformer.h>
#include <normal/executor/physical/sort/SortPOp.h>
#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/aggregate/Sum.h>
#include <normal/executor/physical/group/GroupPOp.h>
#include <normal/executor/physical/project/ProjectPOp.h>
#include <normal/executor/physical/filter/FilterPOp.h>
#include <normal/executor/physical/hashjoin/HashJoinBuildPOp.h>
#include <normal/executor/physical/hashjoin/HashJoinProbePOp.h>
#include <normal/executor/physical/hashjoin/HashJoinPredicate.h>
#include <normal/executor/physical/shuffle/ShufflePOp.h>
#include <normal/executor/physical/collate/CollatePOp.h>
#include <normal/expression/gandiva/Column.h>

namespace normal::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<AWSClient> &awsClient,
                                       const shared_ptr<Mode> &mode,
                                       long queryId,
                                       int parallelDegree) :
  prePhysicalPlan_(prePhysicalPlan),
  awsClient_(awsClient),
  mode_(mode),
  queryId_(queryId),
  parallelDegree_(parallelDegree) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // transform from root in dfs
  auto rootTransRes = transformDfs(prePhysicalPlan_->getRootOp());
  auto upConnPOps = rootTransRes.first;
  auto allPOps = rootTransRes.second;

  // make a collate operator
  shared_ptr<PhysicalOp> collatePOp = make_shared<collate::CollatePOp>("collate", queryId_);
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

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<sort::SortPOp>(fmt::format("Sort-{}", index),
                                                     projectColumnNames,
                                                     queryId_));
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for aggregate, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // aggregate functions
  vector<shared_ptr<aggregate::AggregationFunction>> aggFunctions, aggReduceFunctions;
  for (size_t i = 0; i < aggregatePrePOp->getFunctions().size(); ++i) {
    const auto &prepFunction = aggregatePrePOp->getFunctions()[i];
    const auto alias = aggregatePrePOp->getAggOutputColumnNames()[i];
    aggFunctions.emplace_back(transformAggFunction(alias, prepFunction));
    aggReduceFunctions.emplace_back(transformAggReduceFunction(alias, prepFunction));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<aggregate::AggregatePOp>(fmt::format("Aggregate-{}", index),
                                                               projectColumnNames,
                                                               aggFunctions,
                                                               queryId_));
  }

  // if num > 1, then we need a aggregate reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = selfPOps;
    selfConnDownPOps = selfPOps;
  } else {
    shared_ptr<PhysicalOp> aggReducePOp = make_shared<aggregate::AggregatePOp>("AggregateReduce",
                                                                               projectColumnNames,
                                                                               aggReduceFunctions,
                                                                               queryId_);
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
  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // aggregate functions
  vector<shared_ptr<aggregate::AggregationFunction>> aggFunctions, aggReduceFunctions;
  for (size_t i = 0; i < groupPrePOp->getFunctions().size(); ++i) {
    const auto &prepFunction = groupPrePOp->getFunctions()[i];
    const auto alias = groupPrePOp->getAggOutputColumnNames()[i];
    aggFunctions.emplace_back(transformAggFunction(alias, prepFunction));
    aggReduceFunctions.emplace_back(transformAggReduceFunction(alias, prepFunction));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                    groupPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group-{}", index),
                                                       groupPrePOp->getGroupColumnNames(),
                                                       groupPrePOp->getAggOutputColumnNames(),
                                                       aggFunctions,
                                                       projectColumnNames,
                                                       queryId_));
  }

  // if num > 1, then we need a group reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = selfPOps;
    selfConnDownPOps = selfPOps;
  } else {
    shared_ptr<PhysicalOp> groupReducePOp = make_shared<group::GroupPOp>("GroupReduce",
                                                                         groupPrePOp->getGroupColumnNames(),
                                                                         groupPrePOp->getAggOutputColumnNames(),
                                                                         aggReduceFunctions,
                                                                         projectColumnNames,
                                                                         queryId_);
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
    selfPOps.emplace_back(make_shared<project::ProjectPOp>(fmt::format("Project-{}", index),
                                                     projectPrePOp->getExprs(),
                                                     vector<string>{},  // FIXME: add exprNames to ProjectPrePOp
                                                     projectColumnNames,
                                                     queryId_));
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {
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
    selfPOps.emplace_back(make_shared<filter::FilterPOp>(fmt::format("Filter-{}", index),
                                                         filterPrePOp->getPredicate(),
                                                         nullptr,
                                                         projectColumnNames,
                                                         queryId_));
  }

  // connect to upstream
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // transform producers
  const auto &producersTransRes = transformProducers(hashJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for filter, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }

  auto leftTransRes = producersTransRes[0];
  auto rightTransRes = producersTransRes[1];
  auto allPOps = leftTransRes.second;
  allPOps.insert(allPOps.end(), rightTransRes.second.begin(), rightTransRes.second.end());

  // transform self
  vector<string> projectColumnNames{hashJoinPrePOp->getProjectColumnNames().begin(),
                                    hashJoinPrePOp->getProjectColumnNames().end()};
  // FIXME: support multiple pairs of join columns
  const auto leftColumnName = hashJoinPrePOp->getLeftColumnNames()[0];
  const auto rightColumnName = hashJoinPrePOp->getRightColumnNames()[0];

  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
  for (int i = 0; i < parallelDegree_; ++i) {
    hashJoinBuildPOps.emplace_back(make_shared<hashjoin::HashJoinBuildPOp>(
            fmt::format("HashJoinBuild-{}-{}-{}", leftColumnName, rightColumnName, i),
            leftColumnName,
            projectColumnNames,
            queryId_));
    hashJoinProbePOps.emplace_back(make_shared<hashjoin::HashJoinProbePOp>(
            fmt::format("HashJoinProbe-{}-{}-{}", leftColumnName, rightColumnName, i),
            hashjoin::HashJoinPredicate::create(leftColumnName, rightColumnName),
            projectColumnNames,
            queryId_));
  }
  allPOps.insert(allPOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
  allPOps.insert(allPOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());

  // if num > 1, then we need shuffle operators
  if (parallelDegree_ == 1) {
    // connect to upstream
    connectManyToOne(leftTransRes.first, hashJoinBuildPOps[0]);
    connectManyToOne(rightTransRes.first, hashJoinProbePOps[0]);
  } else {
    vector<shared_ptr<PhysicalOp>> shuffleLeftPOps, shuffleRightPOps;
    for (const auto &upLeftConnPOp: leftTransRes.first) {
      shuffleLeftPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle-{}", upLeftConnPOp->name()),
              leftColumnName,
              upLeftConnPOp->getProjectColumnNames(),
              queryId_));
    }
    for (const auto &upRightConnPOp: rightTransRes.first) {
      shuffleRightPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle-{}", upRightConnPOp->name()),
              rightColumnName,
              upRightConnPOp->getProjectColumnNames(),
              queryId_));
    }
    allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());
    connectManyToMany(shuffleLeftPOps, hashJoinBuildPOps);
    connectManyToMany(shuffleRightPOps, hashJoinProbePOps);

    // connect to upstream
    connectOneToOne(leftTransRes.first, shuffleLeftPOps);
    connectOneToOne(rightTransRes.first, shuffleRightPOps);
  };

  return make_pair(hashJoinProbePOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto s3PTransformer = make_shared<PrePToS3PTransformer>(awsClient_, mode_, queryId_);
  return s3PTransformer->transformFilterableScan(filterableScanPrePOp);
}

shared_ptr<aggregate::AggregationFunction>
PrePToPTransformer::transformAggFunction(const string &alias,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(alias, prePFunction->getExpression());
    }
    case plan::prephysical::COUNT:
    case plan::prephysical::MIN:
    case plan::prephysical::MAX:
    case plan::prephysical::AVG:
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregationFunction>
PrePToPTransformer::transformAggReduceFunction(const string &alias,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(alias, normal::expression::gandiva::col(alias));
    }
    case plan::prephysical::COUNT:
    case plan::prephysical::MIN:
    case plan::prephysical::MAX:
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
