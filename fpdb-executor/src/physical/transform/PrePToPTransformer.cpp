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
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePreparePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateMergePOp.h>
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
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_PARALLEL_SUM_COLUMN_PREFIX + aggOutputColumnName);
        parallelAggProjectColumnNames.emplace_back(AggregatePrePFunction::AVG_PARALLEL_COUNT_COLUMN_PREFIX + aggOutputColumnName);
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
    PrePToPTransformerUtil::connectManyToMany(shufflePOps, groupPOps);
    allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());

    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upConnPOps, shufflePOps);
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return groupPOps;
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

  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
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

  // if using bloom filter, and bloom filter cannot be used before right join
  if (USE_BLOOM_FILTER &&
    (joinType == JoinType::INNER || joinType == JoinType::LEFT || joinType == JoinType::SEMI)) {
    // BloomFilterCreatePreparePOp
    shared_ptr<PhysicalOp> bloomFilterCreatePreparePOp = make_shared<bloomfilter::BloomFilterCreatePreparePOp>(
            fmt::format("BloomFilterCreatePrepare[{}]-{}", prePOpId, hashJoinPredicateStr),
            std::vector<std::string>{},         // not needed
            rand() % numNodes_);
    allPOps.emplace_back(bloomFilterCreatePreparePOp);

    // Connect to upstream as a special type of op
    for (const auto &upLeftConnPOp: upLeftConnPOps) {
      upLeftConnPOp->setBloomFilterCreatePrepareConsumer(bloomFilterCreatePreparePOp);
      bloomFilterCreatePreparePOp->consume(upLeftConnPOp);
    }

    // BloomFilterCreatePOp
    vector<shared_ptr<PhysicalOp>> bloomFilterCreatePOps;
    for (uint i = 0; i < upLeftConnPOps.size(); ++i) {
      bloomFilterCreatePOps.emplace_back(make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              std::vector<std::string>{},
              upLeftConnPOps[i]->getNodeId(),         // not needed
              leftColumnNames));
    }
    allPOps.insert(allPOps.end(), bloomFilterCreatePOps.begin(), bloomFilterCreatePOps.end());
    PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, bloomFilterCreatePOps);
    PrePToPTransformerUtil::connectOneToMany(bloomFilterCreatePreparePOp, bloomFilterCreatePOps);

    // BloomFilterCreateMergePOp if more than one BloomFilterCreatePOp
    shared_ptr<PhysicalOp> finalOpForBloomFilterCreate;
    if (bloomFilterCreatePOps.size() > 1) {
      finalOpForBloomFilterCreate = make_shared<bloomfilter::BloomFilterCreateMergePOp>(
              fmt::format("BloomFilterCreateMerge[{}]-{}", prePOpId, hashJoinPredicateStr),
              std::vector<std::string>{},         // not needed
              rand() % numNodes_);
      allPOps.emplace_back(finalOpForBloomFilterCreate);
      PrePToPTransformerUtil::connectManyToOne(bloomFilterCreatePOps, finalOpForBloomFilterCreate);
    } else {
      finalOpForBloomFilterCreate = bloomFilterCreatePOps[0];
    }

    // BloomFilterUsePOp
    vector<shared_ptr<PhysicalOp>> bloomFilterUsePOps;
    for (uint i = 0; i < upRightConnPOps.size(); ++i) {
      bloomFilterUsePOps.emplace_back(make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse[{}]-{}-{}", prePOpId, hashJoinPredicateStr, i),
              upRightConnPOps[i]->getProjectColumnNames(),
              upRightConnPOps[i]->getNodeId(),
              rightColumnNames));
    }

    // Check if we need to push BloomFilterUsePOp to store
    switch (catalogueEntry_->getType()) {
      case CatalogueEntryType::OBJ_STORE: {
        auto objCatalogueEntry = std::static_pointer_cast<obj_store::ObjStoreCatalogueEntry>(catalogueEntry_);
        pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>> bloomFilterAddRes;

        switch (objCatalogueEntry->getStoreType()) {
          case obj_store::ObjStoreType::FPDB_STORE: {
            bloomFilterAddRes = PrePToFPDBStorePTransformer::addBloomFilterUse(upRightConnPOps,
                                                                               bloomFilterUsePOps,
                                                                               mode_);
            break;
          }
          case obj_store::ObjStoreType::S3: {
            bloomFilterAddRes = PrePToS3PTransformer::addBloomFilterUse(upRightConnPOps,
                                                                        bloomFilterUsePOps,
                                                                        mode_);
            break;
          }
          default: {
            throw std::runtime_error(fmt::format("Unsupported object store type: '{}'", objCatalogueEntry->getStoreTypeName()));
          }
        }

        // opsForBloomFilterUse can be either store op (FPDBStoreSuper/S3Select) containing BloomFilterUse or just BloomFilterUse
        auto opsForBloomFilterUse = bloomFilterAddRes.first;
        auto addiPOps = bloomFilterAddRes.second;

        allPOps.insert(allPOps.end(), addiPOps.begin(), addiPOps.end());
        PrePToPTransformerUtil::connectOneToMany(finalOpForBloomFilterCreate, opsForBloomFilterUse);
        upRightConnPOps = bloomFilterAddRes.first;
        break;
      }
      case CatalogueEntryType::LOCAL_FS: {
        // no bloom filter pushdown
        allPOps.insert(allPOps.end(), bloomFilterUsePOps.begin(), bloomFilterUsePOps.end());
        PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, bloomFilterUsePOps);
        PrePToPTransformerUtil::connectOneToMany(finalOpForBloomFilterCreate, bloomFilterUsePOps);
        upRightConnPOps = bloomFilterUsePOps;
        break;
      }
      default: {
        throw std::runtime_error(fmt::format("Unsupported catalogue entry type: '{}'", catalogueEntry_->getTypeName()));
      }
    }
  }

  // if num > 1, then we need shuffle operators
  if (parallelDegree_ * numNodes_ == 1) {
    PrePToPTransformerUtil::connectManyToOne(upLeftConnPOps, hashJoinBuildPOps[0]);
    PrePToPTransformerUtil::connectManyToOne(upRightConnPOps, hashJoinProbePOps[0]);
  } else {
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
    PrePToPTransformerUtil::connectManyToMany(shuffleLeftPOps, hashJoinBuildPOps);
    PrePToPTransformerUtil::connectManyToMany(shuffleRightPOps, hashJoinProbePOps);
    allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());

    // connect to upstream
    PrePToPTransformerUtil::connectOneToOne(upLeftConnPOps, shuffleLeftPOps);
    PrePToPTransformerUtil::connectOneToOne(upRightConnPOps, shuffleRightPOps);
  }

  // add and return
  PrePToPTransformerUtil::addPhysicalOps(allPOps, physicalOps_);
  return hashJoinProbePOps;
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
