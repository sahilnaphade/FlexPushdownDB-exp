//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/executor/physical/transform/PrePToS3PTransformer.h>
#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/executor/physical/limitsort/LimitSortPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                                       const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                       const shared_ptr<Mode> &mode,
                                       int parallelDegree,
                                       int numNodes) :
  prePhysicalPlan_(prePhysicalPlan),
  catalogueEntry_(catalogueEntry),
  objStoreConnector_(objStoreConnector),
  mode_(mode),
  parallelDegree_(parallelDegree),
  numNodes_(numNodes) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                                       const shared_ptr<CatalogueEntry> &catalogueEntry,
                                                       const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                                       const shared_ptr<Mode> &mode,
                                                       int parallelDegree,
                                                       int numNodes) {
  PrePToPTransformer transformer(prePhysicalPlan, catalogueEntry, objStoreConnector, mode, parallelDegree, numNodes);
  return transformer.transform();
}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // transform from root in dfs
  auto upConnPOps = transformDfs(prePhysicalPlan_->getRootOp());

  // make a collate operator
  shared_ptr<PhysicalOp> collatePOp = make_shared<collate::CollatePOp>(
          "Collate",
          ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()),
          0);
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, collatePOp);
  PrePToPTransformerUtil::addPhysicalOps({collatePOp}, physicalOps_);

  return make_shared<PhysicalPlan>(physicalOps_, collatePOp->name());
}

vector<shared_ptr<PhysicalOp>> PrePToPTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    case PrePOpType::SORT: {
      const auto &sortPrePOp = std::static_pointer_cast<SortPrePOp>(prePOp);
      return transformSort(sortPrePOp);
    }
    case PrePOpType::LIMIT_SORT: {
      const auto &limitSortPrePOp = std::static_pointer_cast<LimitSortPrePOp>(prePOp);
      return transformLimitSort(limitSortPrePOp);
    }
    case PrePOpType::AGGREGATE: {
      const auto &aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(prePOp);
      return transformAggregate(aggregatePrePOp);
    }
    case PrePOpType::GROUP: {
      const auto &groupPrePOp = std::static_pointer_cast<GroupPrePOp>(prePOp);
      return transformGroup(groupPrePOp);
    }
    case PrePOpType::PROJECT: {
      const auto &projectPrePOp = std::static_pointer_cast<ProjectPrePOp>(prePOp);
      return transformProject(projectPrePOp);
    }
    case PrePOpType::FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case PrePOpType::HASH_JOIN: {
      const auto &hashJoinPrePOp = std::static_pointer_cast<HashJoinPrePOp>(prePOp);
      return transformHashJoin(hashJoinPrePOp);
    }
    case PrePOpType::NESTED_LOOP_JOIN: {
      const auto &nestedLoopJoinPrePOp = std::static_pointer_cast<NestedLoopJoinPrePOp>(prePOp);
      return transformNestedLoopJoin(nestedLoopJoinPrePOp);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    case PrePOpType::SEPARABLE_SUPER: {
      const auto &separableSuperPrePOp = std::static_pointer_cast<SeparableSuperPrePOp>(prePOp);
      return transformSeparableSuper(separableSuperPrePOp);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

vector<vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<vector<shared_ptr<PhysicalOp>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformSort(const shared_ptr<SortPrePOp> &sortPrePOp) {
  // id
  auto prePOpId = sortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(sortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for sort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> sortPOp = make_shared<sort::SortPOp>(fmt::format("Sort[{}]", prePOpId),
                                                              projectColumnNames,
                                                              rand() % numNodes_,
                                                              sortPrePOp->getSortKeys());

  // connect to upstream
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, sortPOp);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps({sortPOp}, physicalOps_);
  return {sortPOp};
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp) {
  // id
  auto prePOpId = limitSortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(limitSortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for limitSort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<string> projectColumnNames{limitSortPrePOp->getProjectColumnNames().begin(),
                                    limitSortPrePOp->getProjectColumnNames().end()};

  shared_ptr<PhysicalOp> limitSortPOp = make_shared<limitsort::LimitSortPOp>(
          fmt::format("LimitSort[{}]", prePOpId),
          projectColumnNames,
          rand() % numNodes_,
          limitSortPrePOp->getK(),
          limitSortPrePOp->getSortKeys());

  // connect to upstream
  PrePToPTransformerUtil::connectManyToOne(upConnPOps, limitSortPOp);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps({limitSortPOp}, physicalOps_);
  return {limitSortPOp};
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // id
  auto prePOpId = aggregatePrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for aggregate, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  bool isParallel = upConnPOps.size() > 1;
  vector<shared_ptr<PhysicalOp>> allPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  vector<string> parallelAggProjectColumnNames;
  if (isParallel) {
    for (uint i = 0; i < aggregatePrePOp->getFunctions().size(); ++i) {
      const auto prePFunction = aggregatePrePOp->getFunctions()[i];
      const auto aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[i];
      if (prePFunction->getType() == AggregatePrePFunctionType::AVG) {
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
      } else {
        parallelAggProjectColumnNames.emplace_back(aggOutputColumnName);
      }
    }
  } else {
    parallelAggProjectColumnNames = projectColumnNames;
  }

  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = aggregatePrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   isParallel);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    allPOps.emplace_back(make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-{}", prePOpId, i),
            parallelAggProjectColumnNames,
            upConnPOps[i]->getNodeId(),
            aggFunctions));
  }

  // if num > 1, then we need an aggregate reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = allPOps;
    selfConnDownPOps = allPOps;
  } else {
    // aggregate reduce functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
    for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = aggregatePrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[j];
      aggReduceFunctions.emplace_back(PrePToPTransformerUtil::transformAggReduceFunction(aggOutputColumnName,
                                                                                         prePFunction));
    }

    shared_ptr<PhysicalOp> aggReducePOp = make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-Reduce", prePOpId),
            projectColumnNames,
            rand() % numNodes_,
            aggReduceFunctions);
    PrePToPTransformerUtil::connectManyToOne(allPOps, aggReducePOp);
    selfConnUpPOps = allPOps;
    selfConnDownPOps.emplace_back(aggReducePOp);
    allPOps.emplace_back(aggReducePOp);
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, selfConnUpPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return selfConnDownPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  if (USE_TWO_PHASE_GROUP_BY) {
    return transformGroupTwoPhase(groupPrePOp);
  } else {
    return transformGroupOnePhase(groupPrePOp);
  }
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroupOnePhase(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps, groupPOps;
  vector<string> projectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                    groupPrePOp->getProjectColumnNames().end()};
  for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = groupPrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   false);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    groupPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}", prePOpId, i),
                                                        projectColumnNames,
                                                        i % numNodes_,
                                                        groupPrePOp->getGroupColumnNames(),
                                                        aggFunctions));
  }
  allPOps.insert(allPOps.end(), groupPOps.begin(), groupPOps.end());

  // if num > 1, then we add a shuffle stage ahead
  if (parallelDegree_ * numNodes_ == 1) {
    // connect to upstream
    PrePToPTransformerUtil::connectManyToOne(upConnPOps, groupPOps[0]);
  } else {
    vector<shared_ptr<PhysicalOp>> shufflePOps;
    for (const auto &upConnPOp: upConnPOps) {
      shufflePOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upConnPOp->name()),
              upConnPOp->getProjectColumnNames(),
              upConnPOp->getNodeId(),
              groupPrePOp->getGroupColumnNames()));
    }

    // check if we need to push shuffle to store
    if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
        std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
          obj_store::ObjStoreType::FPDB_STORE) {
      const auto &shuffleAddRes = PrePToFPDBStorePTransformer::addSeparablePOp(upConnPOps, shufflePOps, mode_);
      auto opsForShuffle = shuffleAddRes.first;
      auto addiOps = shuffleAddRes.second;
      allPOps.insert(allPOps.end(), addiOps.begin(), addiOps.end());
      upConnPOps = opsForShuffle;
    } else {
      allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());
      // connect to upstream
      PrePToPTransformerUtil::connectOneToOne(upConnPOps, shufflePOps);
      upConnPOps = shufflePOps;
    }

    // connect to downstream
    PrePToPTransformerUtil::connectManyToMany(upConnPOps, groupPOps);
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return groupPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformGroupTwoPhase(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];
  bool isParallel = upConnPOps.size() > 1;

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps, selfConnDownPOps;;
  vector<string> finalProjectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                         groupPrePOp->getProjectColumnNames().end()};
  vector<string> parallelGroupProjectColumnNames;
  if (isParallel) {
    parallelGroupProjectColumnNames.insert(parallelGroupProjectColumnNames.end(),
                                           groupPrePOp->getGroupColumnNames().begin(),
                                           groupPrePOp->getGroupColumnNames().end());
    for (uint i = 0; i < groupPrePOp->getFunctions().size(); ++i) {
      const auto prePFunction = groupPrePOp->getFunctions()[i];
      const auto aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[i];
      if (prePFunction->getType() == AggregatePrePFunctionType::AVG) {
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelGroupProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + aggOutputColumnName);
      } else {
        parallelGroupProjectColumnNames.emplace_back(aggOutputColumnName);
      }
    }
  } else {
    parallelGroupProjectColumnNames = finalProjectColumnNames;
  }

  // phase 1: parallel group ops
  vector<shared_ptr<PhysicalOp>> groupPOps;
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
      const auto &prePFunction = groupPrePOp->getFunctions()[j];
      const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
      const auto &transAggFunctions = PrePToPTransformerUtil::transformAggFunction(aggOutputColumnName,
                                                                                   prePFunction,
                                                                                   isParallel);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }

    groupPOps.emplace_back(make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}", prePOpId, i),
                                                        parallelGroupProjectColumnNames,
                                                        upConnPOps[i]->getNodeId(),
                                                        groupPrePOp->getGroupColumnNames(),
                                                        aggFunctions));
  }
  allPOps.insert(allPOps.end(), groupPOps.begin(), groupPOps.end());

  // if num > 1, then we need make a shuffle stage after parallel group ops and then add parallel group reduce ops
  if (!isParallel) {
    // connect to upstream
    PrePToPTransformerUtil::connectManyToOne(upConnPOps, groupPOps[0]);
    selfConnDownPOps = groupPOps;
  } else {
    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upConnPOps, groupPOps);

    // shuffle ops
    vector<shared_ptr<PhysicalOp>> shufflePOps;
    shufflePOps.reserve(groupPOps.size());
    for (const auto &groupPOp: groupPOps) {
      shufflePOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, groupPOp->name()),
              groupPOp->getProjectColumnNames(),
              groupPOp->getNodeId(),
              groupPrePOp->getGroupColumnNames()));
    }
    allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());
    PrePToPTransformerUtil::connectOneToOne(groupPOps, shufflePOps);

    // phase 2: parallel group reduce ops
    vector<shared_ptr<PhysicalOp>> groupReducePOps;
    groupReducePOps.reserve(parallelDegree_ * numNodes_);
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      // aggregate reduce functions, better to let each operator has its own copy of aggregate functions
      vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
      for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
        const auto &prePFunction = groupPrePOp->getFunctions()[j];
        const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
        aggReduceFunctions.emplace_back(PrePToPTransformerUtil::transformAggReduceFunction(aggOutputColumnName,
                                                                                           prePFunction));
      }

      groupReducePOps.emplace_back(make_shared<group::GroupPOp>(
              fmt::format("Group[{}]-reduce-{}", prePOpId, i),
              finalProjectColumnNames,
              i % numNodes_,
              groupPrePOp->getGroupColumnNames(),
              aggReduceFunctions));
    }
    allPOps.insert(allPOps.end(), groupReducePOps.begin(), groupReducePOps.end());
    PrePToPTransformerUtil::connectManyToMany(shufflePOps, groupReducePOps);
    selfConnDownPOps = groupReducePOps;
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return selfConnDownPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp) {
  // id
  auto prePOpId = projectPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(projectPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for project, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{projectPrePOp->getProjectColumnNames().begin(),
                                    projectPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    allPOps.emplace_back(make_shared<project::ProjectPOp>(fmt::format("Project[{}]-{}", prePOpId, i),
                                                          projectColumnNames,
                                                          upConnPOps[i]->getNodeId(),
                                                          projectPrePOp->getExprs(),
                                                          projectPrePOp->getExprNames(),
                                                          projectPrePOp->getProjectColumnNamePairs()));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, allPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return allPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {
  // id
  auto prePOpId = filterPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(filterPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for filter, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }
  auto upConnPOps = producersTransRes[0];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{filterPrePOp->getProjectColumnNames().begin(),
                                    filterPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    allPOps.emplace_back(make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}", prePOpId, i),
                                                        projectColumnNames,
                                                        upConnPOps[i]->getNodeId(),
                                                        filterPrePOp->getPredicate(),
                                                        nullptr));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, allPOps);

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return allPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // id
  auto prePOpId = hashJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(hashJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for hashJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }
  auto upLeftConnPOps = producersTransRes[0];
  auto upRightConnPOps = producersTransRes[1];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{hashJoinPrePOp->getProjectColumnNames().begin(),
                                    hashJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = hashJoinPrePOp->getJoinType();
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();
  join::HashJoinPredicate hashJoinPredicate(leftColumnNames, rightColumnNames);
  const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

  // hash join operators, if using arrow's impl, we make hashJoinArrowPOps,
  // otherwise we make hashJoinBuildPOps and hashJoinProbePOps
  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
  vector<shared_ptr<PhysicalOp>> hashJoinArrowPOps;
  if (USE_ARROW_HASH_JOIN_IMPL) {
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      hashJoinArrowPOps.emplace_back(make_shared<join::HashJoinArrowPOp>(
              fmt::format("HashJoinArrow[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              hashJoinPredicate,
              joinType));
    }
    allPOps.insert(allPOps.end(), hashJoinArrowPOps.begin(), hashJoinArrowPOps.end());
  } else {
    for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
      hashJoinBuildPOps.emplace_back(make_shared<join::HashJoinBuildPOp>(
              fmt::format("HashJoinBuild[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              leftColumnNames));
      hashJoinProbePOps.emplace_back(make_shared<join::HashJoinProbePOp>(
              fmt::format("HashJoinProbe[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              projectColumnNames,
              i % numNodes_,
              hashJoinPredicate,
              joinType));
    }
    allPOps.insert(allPOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
    allPOps.insert(allPOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());
    PrePToPTransformerUtil::connectOneToOne(hashJoinBuildPOps, hashJoinProbePOps);
  }

  // check if using bloom filter, note bloom filter cannot be used for right joins
  bool useBloomFilter = USE_BLOOM_FILTER &&
          (joinType == JoinType::INNER || joinType == JoinType::LEFT || joinType == JoinType::SEMI);

  // check if we need to push bloom filter use to store:
  //   - data is in object store
  //   - object store is fpdb-store
  //   - shuffle is separable
  //   - bloom filter use is separable
  //   - upConnRightPOps are all FPDBStoreSuperPOp
  // note if shuffle pushdown is disabled, we cannot push down bloom filter either
  bool isBloomFilterUseSeparable = true;
  if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
    isBloomFilterUseSeparable = false;
  }
  if (isBloomFilterUseSeparable && static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)
          ->getStoreType() != obj_store::ObjStoreType::FPDB_STORE) {
    isBloomFilterUseSeparable = false;
  }
  if (isBloomFilterUseSeparable && !StoreTransformTraits::FPDBStoreStoreTransformTraits()
          ->isSeparable(POpType::SHUFFLE)) {
    isBloomFilterUseSeparable = false;
  }
  if (isBloomFilterUseSeparable && !StoreTransformTraits::FPDBStoreStoreTransformTraits()
          ->isSeparable(POpType::BLOOM_FILTER_USE)) {
    isBloomFilterUseSeparable = false;
  }
  if (isBloomFilterUseSeparable) {
    for (const auto &upRightConnPOp: upRightConnPOps) {
      if (upRightConnPOp->getType() != POpType::FPDB_STORE_SUPER) {
        isBloomFilterUseSeparable = false;
        break;
      }
    }
  }

  // if num > 1, then we need shuffle operators
  if (parallelDegree_ * numNodes_ == 1) {
    // if using bloom filter
    if (useBloomFilter) {
      auto &joinBuildPOp = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps[0] : hashJoinBuildPOps[0];
      auto &joinProbePOp = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps[0] : hashJoinProbePOps[0];

      // BloomFilterCreatePOp
      shared_ptr<PhysicalOp> bloomFilterCreatePOp = make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate[{}]-{}", prePOpId, hashJoinPredicateStr),
              upLeftConnPOps[0]->getProjectColumnNames(),
              joinBuildPOp->getNodeId(),
              leftColumnNames);
      allPOps.emplace_back(bloomFilterCreatePOp);

      // connect bloom filter create to upstream and downstream (joinBuildPOp)
      PrePToPTransformerUtil::connectManyToOne(upLeftConnPOps, bloomFilterCreatePOp);
      if (USE_ARROW_HASH_JOIN_IMPL) {
        bloomFilterCreatePOp->produce(joinBuildPOp);
        static_pointer_cast<join::HashJoinArrowPOp>(joinBuildPOp)->addBuildProducer(bloomFilterCreatePOp);
      } else {
        PrePToPTransformerUtil::connectOneToOne(bloomFilterCreatePOp, joinBuildPOp);
      }

      if (isBloomFilterUseSeparable) {
        // we can push down bloom filter
        // set bloom filter create info
        auto fpdbStoreConnector = static_pointer_cast<FPDBStoreConnector>(objStoreConnector_);
        static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOp)->setBloomFilterInfo(
                FPDBStoreBloomFilterCreateInfo{(int) upRightConnPOps.size(),
                                               fpdbStoreConnector->getHost(),
                                               fpdbStoreConnector->getFlightPort()});

        // set bloom filter use info (embedded bloom filter) to last sub op inside upConnRightPOps
        for (const auto &upRightConnPOp: upRightConnPOps) {
          auto subPlan = static_pointer_cast<FPDBStoreSuperPOp>(upRightConnPOp)->getSubPlan();
          auto expLast = subPlan->getLast();
          if (!expLast.has_value()) {
            throw runtime_error(expLast.error());
          }
          auto expRoot = subPlan->getRootPOp();
          if (!expRoot.has_value()) {
            throw runtime_error(expRoot.error());
          }
          (*expLast)->addConsumerToBloomFilterInfo((*expRoot)->name(),
                                                   bloomFilterCreatePOp->name(),
                                                   rightColumnNames);
        }

        // connect upRightConnPOps (FPDBStoreSuperPOp) to downstream (joinProbePOp)
        if (USE_ARROW_HASH_JOIN_IMPL) {
          for (const auto &upRightConnPOp: upRightConnPOps) {
            upRightConnPOp->produce(joinProbePOp);
            static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOp)->addProbeProducer(upRightConnPOp);
          }
        } else {
          PrePToPTransformerUtil::connectManyToOne(upRightConnPOps, joinProbePOp);
        }
      } else {
        // we cannot push down bloom filter
        // BloomFilterUsePOp
        shared_ptr<PhysicalOp> bloomFilterUsePOp = make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse[{}]-{}", prePOpId, hashJoinPredicateStr),
                upRightConnPOps[0]->getProjectColumnNames(),
                joinProbePOp->getNodeId(),
                rightColumnNames);
        allPOps.emplace_back(bloomFilterUsePOp);

        // connect bloom filter create to bloom filter use
        static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOp)
                ->addBloomFilterUsePOp(bloomFilterUsePOp);
        bloomFilterUsePOp->consume(bloomFilterCreatePOp);

        // connect bloom filter use to upstream and downstream (joinProbePOp)
        PrePToPTransformerUtil::connectManyToOne(upRightConnPOps, bloomFilterUsePOp);
        if (USE_ARROW_HASH_JOIN_IMPL) {
          bloomFilterUsePOp->produce(joinProbePOp);
          static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOp)->addProbeProducer(bloomFilterUsePOp);
        } else {
          PrePToPTransformerUtil::connectOneToOne(bloomFilterUsePOp, joinProbePOp);
        }
      }
    } else {
      // connect hash join to upstream
      if (USE_ARROW_HASH_JOIN_IMPL) {
        for (const auto &upLeftConnPOp: upLeftConnPOps) {
          upLeftConnPOp->produce(hashJoinArrowPOps[0]);
          static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOps[0])->addBuildProducer(upLeftConnPOp);
        }
        for (const auto &upRightConnPOp: upRightConnPOps) {
          upRightConnPOp->produce(hashJoinArrowPOps[0]);
          static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOps[0])->addProbeProducer(upRightConnPOp);
        }
      } else {
        PrePToPTransformerUtil::connectManyToOne(upLeftConnPOps, hashJoinBuildPOps[0]);
        PrePToPTransformerUtil::connectManyToOne(upRightConnPOps, hashJoinProbePOps[0]);
      }
    }
  } else {
    // create shuffle ops
    vector<shared_ptr<PhysicalOp>> shuffleLeftPOps, shuffleRightPOps;
    for (const auto &upLeftConnPOp: upLeftConnPOps) {
      shuffleLeftPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upLeftConnPOp->name()),
              upLeftConnPOp->getProjectColumnNames(),
              upLeftConnPOp->getNodeId(),
              leftColumnNames));
    }
    for (const auto &upRightConnPOp : upRightConnPOps) {
      shuffleRightPOps.emplace_back(make_shared<shuffle::ShufflePOp>(
              fmt::format("Shuffle[{}]-{}", prePOpId, upRightConnPOp->name()),
              upRightConnPOp->getProjectColumnNames(),
              upRightConnPOp->getNodeId(),
              rightColumnNames));
    }

    // check if we need to push shuffle to store
    if (catalogueEntry_->getType() == CatalogueEntryType::OBJ_STORE &&
        std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_)->getStoreType() ==
          obj_store::ObjStoreType::FPDB_STORE) {
      // left, connection to upstream will be made in addSeparablePOp()
      const auto &shuffleLeftAddRes =
              PrePToFPDBStorePTransformer::addSeparablePOp(upLeftConnPOps, shuffleLeftPOps, mode_);
      auto opsForShuffleLeft = shuffleLeftAddRes.first;
      auto leftAddiOps = shuffleLeftAddRes.second;
      allPOps.insert(allPOps.end(), leftAddiOps.begin(), leftAddiOps.end());
      upLeftConnPOps = opsForShuffleLeft;

      // right, connection to upstream will be made in addSeparablePOp()
      const auto &shuffleRightAddRes =
              PrePToFPDBStorePTransformer::addSeparablePOp(upRightConnPOps, shuffleRightPOps, mode_);
      auto opsForShuffleRight = shuffleRightAddRes.first;
      auto rightAddiOps = shuffleRightAddRes.second;
      allPOps.insert(allPOps.end(), rightAddiOps.begin(), rightAddiOps.end());
      upRightConnPOps = opsForShuffleRight;
    } else {
      allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
      allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());

      // connect to upstream
      PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, shuffleLeftPOps);
      PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, shuffleRightPOps);
      upLeftConnPOps = shuffleLeftPOps;
      upRightConnPOps = shuffleRightPOps;
    }

    // if using bloom filter
    if (useBloomFilter) {
      auto &joinBuildPOps = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinBuildPOps;
      auto &joinProbePOps = USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinProbePOps;
      vector<shared_ptr<PhysicalOp>> bloomFilterCreatePOps, bloomFilterUsePOps;

      // BloomFilterCreatePOp
      for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
        bloomFilterCreatePOps.emplace_back(make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
                upLeftConnPOps[0]->getProjectColumnNames(),
                joinBuildPOps[i]->getNodeId(),
                leftColumnNames));
      }
      allPOps.insert(allPOps.end(), bloomFilterCreatePOps.begin(), bloomFilterCreatePOps.end());

      // connect bloom filter create to upstream and downstream (joinBuildPOps)
      // there are actually two choices to connect BloomFilterCreatePOp into the plan:
      //   1)     producer (node i)              2)            producer (node i)
      //                 |                                        /        \
      //      BloomFilterCreate (node j)           consumer (node j)   BloomFilterCreate (node j)
      //                 |
      //          consumer (node j)
      // because BloomFilterCreatePOp is made at the same node of consumer (joinBuild) instead of producer (shuffle),
      // 2) incurs double network transfer for each cross-node tupleSet message (i != j), so we pick 1) here
      PrePToPTransformerUtil::connectManyToMany(upLeftConnPOps, bloomFilterCreatePOps);
      if (USE_ARROW_HASH_JOIN_IMPL) {
        for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
          bloomFilterCreatePOps[i]->produce(joinBuildPOps[i]);
          static_pointer_cast<join::HashJoinArrowPOp>(joinBuildPOps[i])->addBuildProducer(bloomFilterCreatePOps[i]);
        }
      } else {
        PrePToPTransformerUtil::connectOneToOne(bloomFilterCreatePOps, joinBuildPOps);
      }

      if (isBloomFilterUseSeparable) {
        // we can push down bloom filter
        // set bloom filter create info
        auto fpdbStoreConnector = static_pointer_cast<FPDBStoreConnector>(objStoreConnector_);
        for (const auto &bloomFilterCreatePOp: bloomFilterCreatePOps) {
          static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOp)->setBloomFilterInfo(
                  FPDBStoreBloomFilterCreateInfo{(int) upRightConnPOps.size(),
                                                 fpdbStoreConnector->getHost(),
                                                 fpdbStoreConnector->getFlightPort()});
        }

        // set bloom filter use info (embedded bloom filter) to last sub op inside upConnRightPOps
        for (const auto &upRightConnPOp: upRightConnPOps) {
          auto subPlan = static_pointer_cast<FPDBStoreSuperPOp>(upRightConnPOp)->getSubPlan();
          auto expLast = subPlan->getLast();
          if (!expLast.has_value()) {
            throw runtime_error(expLast.error());
          }
          auto expRoot = subPlan->getRootPOp();
          if (!expRoot.has_value()) {
            throw runtime_error(expRoot.error());
          }
          for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
            (*expLast)->addConsumerToBloomFilterInfo(joinProbePOps[i]->name(),
                                                     bloomFilterCreatePOps[i]->name(),
                                                     rightColumnNames);
          }
        }

        // connect upRightConnPOps (FPDBStoreSuperPOp) to downstream (joinProbePOps)
        if (USE_ARROW_HASH_JOIN_IMPL) {
          for (const auto &joinProbePOp: joinProbePOps) {
            for (const auto &upRightConnPOp: upRightConnPOps) {
              upRightConnPOp->produce(joinProbePOp);
              static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOp)->addProbeProducer(upRightConnPOp);
            }
          }
        } else {
          PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, joinProbePOps);
        }
      } else {
        // we cannot push down bloom filter
        // BloomFilterUsePOp
        for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
          bloomFilterUsePOps.emplace_back(make_shared<bloomfilter::BloomFilterUsePOp>(
                  fmt::format("BloomFilterUse[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
                  upRightConnPOps[0]->getProjectColumnNames(),
                  joinProbePOps[i]->getNodeId(),
                  rightColumnNames));
        }
        allPOps.insert(allPOps.end(), bloomFilterUsePOps.begin(), bloomFilterUsePOps.end());

        // connect bloom filter create to bloom filter use
        for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
          static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bloomFilterCreatePOps[i])
                  ->addBloomFilterUsePOp(bloomFilterUsePOps[i]);
          bloomFilterUsePOps[i]->consume(bloomFilterCreatePOps[i]);
        }

        // connect bloom filter use to upstream and downstream (joinProbePOps)
        PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, bloomFilterUsePOps);
        if (USE_ARROW_HASH_JOIN_IMPL) {
          for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
            bloomFilterUsePOps[i]->produce(joinProbePOps[i]);
            static_pointer_cast<join::HashJoinArrowPOp>(joinProbePOps[i])->addProbeProducer(bloomFilterUsePOps[i]);
          }
        } else {
          PrePToPTransformerUtil::connectOneToOne(bloomFilterUsePOps, joinProbePOps);
        }
      }
    } else {
      // connect hash join to upstream
      if (USE_ARROW_HASH_JOIN_IMPL) {
        for (const auto &hashJoinArrowPOp: hashJoinArrowPOps) {
          for (const auto &upLeftConnPOp: upLeftConnPOps) {
            upLeftConnPOp->produce(hashJoinArrowPOp);
            static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOp)->addBuildProducer(upLeftConnPOp);
          }
          for (const auto &upRightConnPOp: upRightConnPOps) {
            upRightConnPOp->produce(hashJoinArrowPOp);
            static_pointer_cast<join::HashJoinArrowPOp>(hashJoinArrowPOp)->addProbeProducer(upRightConnPOp);
          }
        }
      } else {
        PrePToPTransformerUtil::connectManyToMany(upLeftConnPOps, hashJoinBuildPOps);
        PrePToPTransformerUtil::connectManyToMany(upRightConnPOps, hashJoinProbePOps);
      }
    }
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return USE_ARROW_HASH_JOIN_IMPL ? hashJoinArrowPOps : hashJoinProbePOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp) {
  // id
  auto prePOpId = nestedLoopJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(nestedLoopJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for nestedLoopJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }
  auto upLeftConnPOps = producersTransRes[0];
  auto upRightConnPOps = producersTransRes[1];

  // transform self
  vector<shared_ptr<PhysicalOp>> allPOps;
  vector<string> projectColumnNames{nestedLoopJoinPrePOp->getProjectColumnNames().begin(),
                                    nestedLoopJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = nestedLoopJoinPrePOp->getJoinType();
  optional<shared_ptr<fpdb::expression::gandiva::Expression>> predicate;
  if (nestedLoopJoinPrePOp->getPredicate()) {
    predicate = nestedLoopJoinPrePOp->getPredicate();
  }

  vector<shared_ptr<PhysicalOp>> nestedLoopJoinPOps;
  for (int i = 0; i < parallelDegree_ * numNodes_; ++i) {
    shared_ptr<join::NestedLoopJoinPOp> nestedLoopJoinPOp =
            make_shared<join::NestedLoopJoinPOp>(fmt::format("NestedLoopJoin[{}]-{}", prePOpId, i),
                                                 projectColumnNames,
                                                 i % numNodes_,
                                                 predicate,
                                                 joinType);
    // connect to left inputs
    for (const auto &upLeftConnPOp: upLeftConnPOps) {
      upLeftConnPOp->produce(nestedLoopJoinPOp);
      nestedLoopJoinPOp->addLeftProducer(upLeftConnPOp);
    }
    nestedLoopJoinPOps.emplace_back(nestedLoopJoinPOp);
  }
  allPOps.insert(allPOps.end(), nestedLoopJoinPOps.begin(), nestedLoopJoinPOps.end());

  // connect to right inputs, if num > 1, then we need split operators for right producers
  if (parallelDegree_ * numNodes_ == 1) {
    for (const auto &upRightConnPOp: upRightConnPOps) {
      upRightConnPOp->produce(nestedLoopJoinPOps[0]);
      static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOps[0])->addRightProducer(upRightConnPOp);
    }
  } else {
    vector<shared_ptr<PhysicalOp>> splitPOps;
    for (const auto &upRightConnPOp : upRightConnPOps) {
      shared_ptr<split::SplitPOp> splitPOp = make_shared<split::SplitPOp>(
              fmt::format("Split[{}]-{}", prePOpId, upRightConnPOp->name()),
              upRightConnPOp->getProjectColumnNames(),
              upRightConnPOp->getNodeId());
      splitPOps.emplace_back(splitPOp);

      // Connect splitPOp with all nestedLoopJoinPOps
      for (const auto &nestedLoopJoinPOp: nestedLoopJoinPOps) {
        splitPOp->produce(nestedLoopJoinPOp);
        static_pointer_cast<join::NestedLoopJoinPOp>(nestedLoopJoinPOp)->addRightProducer(splitPOp);
      }
    }
    allPOps.insert(allPOps.end(), splitPOps.begin(), splitPOps.end());

    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, splitPOps);
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return nestedLoopJoinPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &) {
  switch (catalogueEntry_->getType()) {
    case CatalogueEntryType::LOCAL_FS: {
      throw runtime_error(fmt::format("Unsupported catalogue entry type for filterable scan prephysical operator: {}",
                                      catalogueEntry_->getTypeName()));
    }
    case CatalogueEntryType::OBJ_STORE: {
      throw runtime_error(fmt::format("Filterable scan should be contained by separable super prephysical operator for object store"));
    }
    default: {
      throw runtime_error(fmt::format("Unknown catalogue entry type: {}", catalogueEntry_->getTypeName()));
    }
  }
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp) {
  if (catalogueEntry_->getType() != CatalogueEntryType::OBJ_STORE) {
    throw runtime_error(fmt::format("Unsupported catalogue entry type for separable super prephysical operator: {}",
                        catalogueEntry_->getTypeName()));
  }
  auto objStoreCatalogueEntry = std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_);

  switch (objStoreCatalogueEntry->getStoreType()) {
    case ObjStoreType::S3: {
      if (objStoreConnector_->getStoreType() != obj_store::ObjStoreType::S3) {
        throw runtime_error("object store type for catalogue entry and connector mismatch, catalogue entry is 'S3'");
      }
      auto s3Connector = static_pointer_cast<obj_store::S3Connector>(objStoreConnector_);
      auto transformRes = PrePToS3PTransformer::transform(separableSuperPrePOp, mode_, numNodes_, s3Connector);
      PrePToPTransformerUtil::addPhysicalOps(transformRes.second, physicalOps_);
      return transformRes.first;
    }
    case ObjStoreType::FPDB_STORE: {
      if (objStoreConnector_->getStoreType() != obj_store::ObjStoreType::FPDB_STORE) {
        throw runtime_error("object store type for catalogue entry and connector mismatch, catalogue entry is 'FPDB-Store'");
      }
      auto fpdbStoreConnector = static_pointer_cast<obj_store::FPDBStoreConnector>(objStoreConnector_);
      auto transformRes = PrePToFPDBStorePTransformer::transform(separableSuperPrePOp, mode_, numNodes_, fpdbStoreConnector);
      PrePToPTransformerUtil::addPhysicalOps(transformRes.second, physicalOps_);
      return transformRes.first;
    }
    default:
      throw runtime_error(fmt::format("Unsupported object store type for separable super prephysical operator: {}",
                                      objStoreCatalogueEntry->getStoreTypeName()));
  }
}

}
