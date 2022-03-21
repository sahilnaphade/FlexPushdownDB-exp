//
// Created by Yifei Yang on 3/3/22.
//

#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/prune/PartitionPruner.h>
#include <fpdb/executor/physical/file/RemoteFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/catalogue/obj-store/ObjStoreTable.h>

using namespace fpdb::catalogue::obj_store;

namespace fpdb::executor::physical {

PrePToFPDBStorePTransformer::PrePToFPDBStorePTransformer(uint prePOpId,
                                                         const shared_ptr<Mode> &mode,
                                                         int numNodes,
                                                         const std::string &host,
                                                         int fileServicePort,
                                                         int flightPort):
  prePOpId_(prePOpId),
  mode_(mode),
  numNodes_(numNodes),
  host_(host),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp) {
  // transform in DFS
  auto rootPrePOp = separableSuperPrePOp->getRootOp();
  auto transformRes = transformDfs(rootPrePOp);
  auto connPOps = transformRes.first;
  auto allPOps = transformRes.second;

  // make reduce op if needed (e.g. when the end of separable super op is aggregate)
  std::optional<std::shared_ptr<PhysicalOp>> reducePOp = std::nullopt;

  if (connPOps.size() > 1) {
    if (rootPrePOp->getType() == PrePOpType::AGGREGATE) {
      auto aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(rootPrePOp);
      vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                        aggregatePrePOp->getProjectColumnNames().end()};

      // aggregate reduce functions
      vector<shared_ptr<aggregate::AggregateFunction>> aggReduceFunctions;
      for (size_t j = 0; j < aggregatePrePOp->getFunctions().size(); ++j) {
        const auto &prePFunction = aggregatePrePOp->getFunctions()[j];
        const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[j];
        aggReduceFunctions.emplace_back(PrePToPTransformerUtil::transformAggReduceFunction(aggOutputColumnName,
                                                                                           prePFunction));
      }

      reducePOp = make_shared<aggregate::AggregatePOp>(fmt::format("Aggregate[{}]-Reduce", aggregatePrePOp->getId()),
                                                       projectColumnNames,
                                                       0,
                                                       aggReduceFunctions);
    }
  }

  // according to the mode
  switch (mode_->id()) {
    case PULL_UP: {
      // connect operators as usual, check whether has reduce op
      if (!reducePOp.has_value()) {
        return transformRes;
      } else {
        PrePToPTransformerUtil::connectManyToOne(connPOps, *reducePOp);
        allPOps.emplace_back(*reducePOp);
        return make_pair(vector<shared_ptr<PhysicalOp>>{*reducePOp}, allPOps);
      }
    }
    case PUSHDOWN_ONLY: {
      // add collate at the end of each FPDB store super op
      vector<shared_ptr<PhysicalOp>> collatePOps;
      for (size_t i = 0; i < connPOps.size(); ++i) {
        collatePOps.emplace_back(make_shared<collate::CollatePOp>(fmt::format("Collate[{}]-{}", rootPrePOp->getId(), i),
                                                                  connPOps[i]->getProjectColumnNames(),
                                                                  connPOps[i]->getNodeId()));
      }
      PrePToPTransformerUtil::connectOneToOne(connPOps, collatePOps);

      // make FPDB store super op
      unordered_map<string, shared_ptr<PhysicalOp>> opMap;
      for (const auto &op: allPOps) {
        opMap.emplace(op->name(), op);
      }

      vector<shared_ptr<PhysicalOp>> fpdbStoreSuperPOps;
      for (uint i = 0; i < collatePOps.size(); ++i) {
        auto subPlan = PrePToPTransformerUtil::rootOpToPlan(collatePOps[i], opMap);
        fpdbStoreSuperPOps.emplace_back(make_shared<fpdb_store::FPDBStoreSuperPOp>(
                fmt::format("FPDBStoreSuper[{}]-{}", rootPrePOp->getId(), i),
                collatePOps[i]->getProjectColumnNames(),
                collatePOps[i]->getNodeId(),
                subPlan,
                host_,
                flightPort_
                ));
      }

      // check whether has reduce op
      if (!reducePOp.has_value()) {
        return make_pair(fpdbStoreSuperPOps, fpdbStoreSuperPOps);
      } else {
        PrePToPTransformerUtil::connectManyToOne(fpdbStoreSuperPOps, *reducePOp);
        allPOps = fpdbStoreSuperPOps;
        allPOps.emplace_back(*reducePOp);
        return make_pair(vector<shared_ptr<PhysicalOp>>{*reducePOp}, allPOps);
      }
    }
    default:
      throw runtime_error(fmt::format("Unsupported mode for FPDB Store: {}", mode_->toString()));
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::addBloomFilterUse(vector<shared_ptr<PhysicalOp>> &producers,
                                               vector<shared_ptr<PhysicalOp>> &bloomFilterUsePOps,
                                               const shared_ptr<Mode> &mode) {
  if (!StoreTransformTraits::FPDBStoreStoreTransformTraits()->isSeparable(POpType::BLOOM_FILTER_USE)
    || mode->id() == ModeId::PULL_UP) {
    PrePToPTransformerUtil::connectOneToOne(producers, bloomFilterUsePOps);
    return {bloomFilterUsePOps, bloomFilterUsePOps};
  }

  if (mode->id() == ModeId::PUSHDOWN_ONLY) {
    vector<shared_ptr<PhysicalOp>> connPOps, addiPOps;

    for (uint i = 0; i < producers.size(); ++i) {
      auto producer = producers[i];
      auto bloomFilterUsePOp = bloomFilterUsePOps[i];
      if (producer->getType() == POpType::FPDB_STORE_SUPER) {
        auto fpdbStoreSuperPOp = static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(producer);
        fpdbStoreSuperPOp->addAsLastOp(bloomFilterUsePOp);
        connPOps.emplace_back(fpdbStoreSuperPOp);
      } else {
        PrePToPTransformerUtil::connectOneToOne(producer, bloomFilterUsePOp);
        connPOps.emplace_back(bloomFilterUsePOp);
        addiPOps.emplace_back(bloomFilterUsePOp);
      }
    }

    return {connPOps, addiPOps};
  } else {
    throw runtime_error(fmt::format("Unsupported mode for FPDB Store: {}", mode->toString()));
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    case PrePOpType::FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    case PrePOpType::PROJECT: {
      const auto &projectPrePOp = std::static_pointer_cast<ProjectPrePOp>(prePOp);
      return transformProject(projectPrePOp);
    }
    case PrePOpType::FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case PrePOpType::AGGREGATE: {
      const auto &aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(prePOp);
      return transformAggregate(aggregatePrePOp);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type for FPDB store: {}", prePOp->getTypeString()));
    }
  }
}

vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
PrePToFPDBStorePTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto &objStoreTable = std::static_pointer_cast<ObjStoreTable>(filterableScanPrePOp->getTable());
  const auto &partitions = (const vector<shared_ptr<Partition>> &) objStoreTable->getObjStorePartitions();
  const auto &partitionPredicates = PartitionPruner::prune(partitions, filterableScanPrePOp->getPredicate());

  switch (mode_->id()) {
    case PULL_UP:
      return transformFilterableScanPullup(filterableScanPrePOp, partitionPredicates);
    case PUSHDOWN_ONLY:
      return transformFilterableScanPushdownOnly(filterableScanPrePOp, partitionPredicates);
    default:
      throw runtime_error(fmt::format("Unsupported mode for FPDB Store: {}", mode_->toString()));
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                              const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates) {
  vector<shared_ptr<PhysicalOp>> scanPOps, filterPOps;
  const auto &table = filterableScanPrePOp->getTable();
  vector<string> projectColumnNames{filterableScanPrePOp->getProjectColumnNames().begin(),
                                    filterableScanPrePOp->getProjectColumnNames().end()};

  /**
   * For each partition, construct:
   * a RemoteFileScan, a Filter if needed
   */

  uint partitionId = 0;
  for (const auto &partitionPredicateIt: partitionPredicates) {
    const auto &partition = static_pointer_cast<ObjStorePartition>(partitionPredicateIt.first);
    const auto &predicate = partitionPredicateIt.second;
    const auto &bucket = partition->getBucket();
    const auto &object = partition->getObject();

    // project column names and its union with project column names
    vector<string> predicateColumnNames;
    if (predicate) {
      const auto predicateColumnNameSet = predicate->involvedColumnNames();
      predicateColumnNames.assign(predicateColumnNameSet.begin(), predicateColumnNameSet.end());
    }
    const auto &projPredColumnNames = union_(projectColumnNames, predicateColumnNames);

    // remote file scan
    const auto &scanPOp = make_shared<file::RemoteFileScanPOp>(fmt::format("RemoteFileScan[{}]-{}/{}", prePOpId_, bucket, object),
                                                               projPredColumnNames,
                                                               partitionId % numNodes_,
                                                               bucket,
                                                               object,
                                                               table->getFormat(),
                                                               table->getSchema(),
                                                               host_,
                                                               fileServicePort_);
    scanPOps.emplace_back(scanPOp);

    // filter
    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}/{}", prePOpId_, bucket, object),
                                                             projectColumnNames,
                                                             partitionId % numNodes_,
                                                             predicate,
                                                             table);
      filterPOps.emplace_back(filterPOp);
      scanPOp->produce(filterPOp);
      filterPOp->consume(scanPOp);
    }

    ++partitionId;
  }

  if (filterPOps.empty()) {
    return make_pair(scanPOps, scanPOps);
  } else {
    vector<shared_ptr<PhysicalOp>> allPOps;
    allPOps.insert(allPOps.end(), scanPOps.begin(), scanPOps.end());
    allPOps.insert(allPOps.end(), filterPOps.begin(), filterPOps.end());
    return make_pair(filterPOps, allPOps);
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformFilterableScanPushdownOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                    const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates) {
  vector<shared_ptr<PhysicalOp>> scanPOps, filterPOps;
  const auto &table = filterableScanPrePOp->getTable();
  vector<string> projectColumnNames{filterableScanPrePOp->getProjectColumnNames().begin(),
                                    filterableScanPrePOp->getProjectColumnNames().end()};

  /**
   * For each partition, construct:
   * a FPDBStoreFileScan, a Filter if needed
   */

  uint partitionId = 0;
  for (const auto &partitionPredicateIt: partitionPredicates) {
    const auto &partition = static_pointer_cast<ObjStorePartition>(partitionPredicateIt.first);
    const auto &predicate = partitionPredicateIt.second;
    const auto &bucket = partition->getBucket();
    const auto &object = partition->getObject();

    // project column names and its union with project column names
    vector<string> predicateColumnNames;
    if (predicate) {
      const auto predicateColumnNameSet = predicate->involvedColumnNames();
      predicateColumnNames.assign(predicateColumnNameSet.begin(), predicateColumnNameSet.end());
    }
    const auto &projPredColumnNames = union_(projectColumnNames, predicateColumnNames);

    // FPDB Store file scan
    const auto &scanPOp = make_shared<fpdb_store::FPDBStoreFileScanPOp>(fmt::format("FPDBStoreFileScan[{}]-{}/{}", prePOpId_, bucket, object),
                                                                        projPredColumnNames,
                                                                        partitionId % numNodes_,
                                                                        bucket,
                                                                        object,
                                                                        table->getFormat(),
                                                                        table->getSchema());
    scanPOps.emplace_back(scanPOp);

    // filter
    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}/{}", prePOpId_, bucket, object),
                                                             projectColumnNames,
                                                             partitionId % numNodes_,
                                                             predicate,
                                                             table);
      filterPOps.emplace_back(filterPOp);
      scanPOp->produce(filterPOp);
      filterPOp->consume(scanPOp);
    }

    ++partitionId;
  }

  if (filterPOps.empty()) {
    return make_pair(scanPOps, scanPOps);
  } else {
    vector<shared_ptr<PhysicalOp>> allPOps;
    allPOps.insert(allPOps.end(), scanPOps.begin(), scanPOps.end());
    allPOps.insert(allPOps.end(), filterPOps.begin(), filterPOps.end());
    return make_pair(filterPOps, allPOps);
  }
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp) {
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
                                                           projectColumnNames,
                                                           upConnPOps[i]->getNodeId(),
                                                           projectPrePOp->getExprs(),
                                                           projectPrePOp->getExprNames(),
                                                           projectPrePOp->getProjectColumnNamePairs()));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {
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
                                                         projectColumnNames,
                                                         upConnPOps[i]->getNodeId(),
                                                         filterPrePOp->getPredicate(),
                                                         nullptr));
  }

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
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
  bool isParallel = upConnPOps.size() > 1;

  vector<shared_ptr<PhysicalOp>> aggregatePOps;
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

    aggregatePOps.emplace_back(make_shared<aggregate::AggregatePOp>(
            fmt::format("Aggregate[{}]-{}", prePOpId, i),
            parallelAggProjectColumnNames,
            upConnPOps[i]->getNodeId(),
            aggFunctions));
  }
  allPOps.insert(allPOps.end(), aggregatePOps.begin(), aggregatePOps.end());

  // connect to upstream
  PrePToPTransformerUtil::connectOneToOne(upConnPOps, aggregatePOps);

  return make_pair(aggregatePOps, allPOps);
}

}
