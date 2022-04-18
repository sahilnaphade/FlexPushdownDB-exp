//
// Created by Yifei Yang on 3/3/22.
//

#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/prune/PartitionPruner.h>
#include <fpdb/executor/physical/file/RemoteFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOpUtil.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/cache/CacheLoadPOp.h>
#include <fpdb/executor/physical/merge/MergePOp.h>
#include <fpdb/catalogue/obj-store/ObjStoreTable.h>

using namespace fpdb::catalogue::obj_store;

namespace fpdb::executor::physical {

PrePToFPDBStorePTransformer::PrePToFPDBStorePTransformer(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
                                                         const shared_ptr<Mode> &mode,
                                                         int numNodes,
                                                         const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector):
  separableSuperPrePOp_(separableSuperPrePOp),
  mode_(mode),
  numNodes_(numNodes),
  fpdbStoreConnector_(fpdbStoreConnector) {}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToFPDBStorePTransformer::transform(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
                                       const shared_ptr<Mode> &mode,
                                       int numNodes,
                                       const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector) {
  PrePToFPDBStorePTransformer transformer(separableSuperPrePOp, mode, numNodes, fpdbStoreConnector);
  return transformer.transform();
} 

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>> PrePToFPDBStorePTransformer::transform() {
  // transform in DFS
  auto rootPrePOp = separableSuperPrePOp_->getRootOp();
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
    case PULL_UP:
    case CACHING_ONLY: {
      // connect operators as usual, check whether has reduce op
      if (!reducePOp.has_value()) {
        return transformRes;
      } else {
        PrePToPTransformerUtil::connectManyToOne(connPOps, *reducePOp);
        allPOps.emplace_back(*reducePOp);
        return make_pair(vector<shared_ptr<PhysicalOp>>{*reducePOp}, allPOps);
      }
    }
    case PUSHDOWN_ONLY:
    case HYBRID: {
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
                fpdbStoreConnector_->getHost(),
                fpdbStoreConnector_->getFlightPort()));
      }

      // transform to hybrid execution plan, if needed
      if (mode_->id() == HYBRID) {
        auto hybridTransformRes = transformPushdownOnlyToHybrid(fpdbStoreSuperPOps);
        connPOps = hybridTransformRes.first;
        allPOps = hybridTransformRes.second;
      } else {
        connPOps = fpdbStoreSuperPOps;
        allPOps = fpdbStoreSuperPOps;
      }

      // check whether has reduce op
      if (!reducePOp.has_value()) {
        return make_pair(connPOps, allPOps);
      } else {
        PrePToPTransformerUtil::connectManyToOne(connPOps, *reducePOp);
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
  // if not in a mode with pushdown, or fpdb-store does not enable bloom filter pushdown
  if (!StoreTransformTraits::FPDBStoreStoreTransformTraits()->isSeparable(POpType::BLOOM_FILTER_USE)
    || mode->id() == ModeId::PULL_UP || mode->id() == ModeId::CACHING_ONLY) {
    PrePToPTransformerUtil::connectOneToOne(producers, bloomFilterUsePOps);
    return {bloomFilterUsePOps, bloomFilterUsePOps};
  }

  // bloom filter can be pushed
  if (mode->id() == ModeId::PUSHDOWN_ONLY) {
    vector<shared_ptr<PhysicalOp>> connPOps, addiPOps;

    for (uint i = 0; i < producers.size(); ++i) {
      auto producer = producers[i];
      auto bloomFilterUsePOp = bloomFilterUsePOps[i];

      // only pushable when the producer is FPDBStoreSuperPOp
      if (producer->getType() == POpType::FPDB_STORE_SUPER) {
        auto fpdbStoreSuperPOp = static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(producer);
        auto res = fpdbStoreSuperPOp->getSubPlan()->addAsLast(bloomFilterUsePOp);
        if (!res.has_value()) {
          throw runtime_error(res.error());
        }
        connPOps.emplace_back(fpdbStoreSuperPOp);
      } else {
        PrePToPTransformerUtil::connectOneToOne(producer, bloomFilterUsePOp);
        connPOps.emplace_back(bloomFilterUsePOp);
        addiPOps.emplace_back(bloomFilterUsePOp);
      }
    }

    return {connPOps, addiPOps};
  } else {
    throw runtime_error(fmt::format("Unsupported mode for push bloom filters to FPDB Store: {}", mode->toString()));
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
PrePToFPDBStorePTransformer::transformPushdownOnlyToHybrid(const vector<shared_ptr<PhysicalOp>> &fpdbStoreSuperPOps) {
  vector<shared_ptr<PhysicalOp>> connPOps, allPOps;
  if (fpdbStoreSuperPOps.empty()) {
    return {connPOps, allPOps};
  }

  // get projectColumnGroups needed for hybrid execution, this is same for all fpdbStoreSuperPOps
  auto projectColumnGroups = fpdb_store::FPDBStoreSuperPOpUtil::getProjectColumnGroups(
          static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(fpdbStoreSuperPOps[0]));

  for (const auto &fpdbStoreSuperPOp: fpdbStoreSuperPOps) {
    auto typedFPDBStoreSuperPOp = static_pointer_cast<fpdb_store::FPDBStoreSuperPOp>(fpdbStoreSuperPOp);

    // set waitForScanMessage, so it won't run until receiving scan message
    typedFPDBStoreSuperPOp->setWaitForScanMessage(true);

    unordered_map<shared_ptr<PhysicalOp>, shared_ptr<PhysicalOp>> storePOpToLocalPOp;
    unordered_map<string, shared_ptr<PhysicalOp>> storePOpMap = typedFPDBStoreSuperPOp->getSubPlan()->getPhysicalOps();
    unordered_map<string, shared_ptr<PhysicalOp>> localPOpMap;
    unordered_map<string, string> storePOpRenames;
    optional<shared_ptr<merge::MergePOp>> mergePOp1, mergePOp2;

    // get predicateColumnNames needed for hybrid execution, this may be different for some fpdbStoreSuperPOps
    // so we need to get it for each one
    auto predicateColumnNames = fpdb_store::FPDBStoreSuperPOpUtil::getPredicateColumnNames(typedFPDBStoreSuperPOp);

    // create mirror ops which are executed locally
    for (const auto &storePOpIt: storePOpMap) {
      auto storePOp = storePOpIt.second;
      if (storePOp->getType() == POpType::COLLATE) {
        continue;
      }

      shared_ptr<PhysicalOp> localPOp;
      bool isMirrored = false;
      switch (storePOp->getType()) {
        case POpType::FPDB_STORE_FILE_SCAN: {
          auto fpdbStoreFileScanPOp = static_pointer_cast<fpdb_store::FPDBStoreFileScanPOp>(storePOp);
          auto bucket = fpdbStoreFileScanPOp->getBucket();
          auto object = fpdbStoreFileScanPOp->getObject();
          auto kernel = fpdbStoreFileScanPOp->getKernel();
          auto fileSize = kernel->getFileSize();

          auto cacheLoadPOp = make_shared<cache::CacheLoadPOp>(fmt::format("CacheLoad[{}]-{}/{}",
                                                                           separableSuperPrePOp_->getId(),
                                                                           bucket,
                                                                           object),
                                                               fpdbStoreFileScanPOp->getProjectColumnNames(),
                                                               fpdbStoreFileScanPOp->getNodeId(),
                                                               predicateColumnNames,
                                                               projectColumnGroups,
                                                               fpdbStoreFileScanPOp->getProjectColumnNames(),
                                                               make_shared<ObjStorePartition>(bucket, object, fileSize),
                                                               0,
                                                               fileSize,
                                                               fpdbStoreConnector_);
          localPOp = cacheLoadPOp;

          // Create a RemoteFileScanPOp to load data to cache, and a MergePOp, then wire them up
          auto missPOpToCache = make_shared<file::RemoteFileScanPOp>(fmt::format("RemoteFileScan(cache)[{}]-{}/{}",
                                                                                 separableSuperPrePOp_->getId(),
                                                                                 bucket,
                                                                                 object),
                                                                     fpdbStoreFileScanPOp->getProjectColumnNames(),
                                                                     fpdbStoreFileScanPOp->getNodeId(),
                                                                     bucket,
                                                                     object,
                                                                     kernel->getFormat(),
                                                                     kernel->getSchema(),
                                                                     fileSize,
                                                                     fpdbStoreConnector_->getHost(),
                                                                     fpdbStoreConnector_->getFileServicePort(),
                                                                     nullopt,
                                                                     false,
                                                                     true);
          // first merge for cached data and to-cache data
          mergePOp1 = make_shared<merge::MergePOp>(fmt::format("merge1[{}]-{}/{}",
                                                               separableSuperPrePOp_->getId(),
                                                               bucket,
                                                               object),
                                                   fpdbStoreFileScanPOp->getProjectColumnNames(),
                                                   fpdbStoreFileScanPOp->getNodeId());
          cacheLoadPOp->setHitOperator(*mergePOp1);
          (*mergePOp1)->setLeftProducer(cacheLoadPOp);
          cacheLoadPOp->setMissOperatorToCache(missPOpToCache);
          missPOpToCache->consume(cacheLoadPOp);
          missPOpToCache->produce(*mergePOp1);
          (*mergePOp1)->setRightProducer(missPOpToCache);

          // miss operator to push down
          cacheLoadPOp->setMissOperatorToPushdown(fpdbStoreSuperPOp);
          fpdbStoreSuperPOp->consume(cacheLoadPOp);

          // second merge for local result and pushdown result
          mergePOp2 = make_shared<merge::MergePOp>(fmt::format("merge2[{}]-{}/{}",
                                                               separableSuperPrePOp_->getId(),
                                                               bucket,
                                                               object),
                                                   fpdbStoreSuperPOp->getProjectColumnNames(),
                                                   fpdbStoreSuperPOp->getNodeId());

          // save created ops
          connPOps.emplace_back(*mergePOp2);
          allPOps.emplace_back(missPOpToCache);
          allPOps.emplace_back(fpdbStoreSuperPOp);
          allPOps.emplace_back(*mergePOp1);
          allPOps.emplace_back(*mergePOp2);

          break;
        }
        case POpType::FILTER: {
          localPOp = make_shared<filter::FilterPOp>(dynamic_cast<filter::FilterPOp &>(*storePOp));
          isMirrored = true;
          break;
        }
        case POpType::PROJECT: {
          localPOp = make_shared<project::ProjectPOp>(dynamic_cast<project::ProjectPOp &>(*storePOp));
          isMirrored = true;
          break;
        }
        case POpType::AGGREGATE: {
          localPOp = make_shared<aggregate::AggregatePOp>(dynamic_cast<aggregate::AggregatePOp &>(*storePOp));
          isMirrored = true;
          break;
        }
        default: {
          throw runtime_error(fmt::format("Unsupported physical operator type at FPDB store: {}", storePOp->getTypeString()));
        }
      }

      // clear connections and set separated for mirrored operators
      if (isMirrored) {
        localPOp->clearConnections();
        localPOp->setName(fmt::format("{}-separated(local)", localPOp->name()));
        localPOp->setSeparated(true);
        storePOpRenames.emplace(storePOp->name(), fmt::format("{}-separated(fpdb-store)", storePOp->name()));
        storePOp->setSeparated(true);
      }

      storePOpToLocalPOp.emplace(storePOp, localPOp);
      localPOpMap.emplace(localPOp->name(), localPOp);
      allPOps.emplace_back(localPOp);
    }

    if (!mergePOp1.has_value() || !mergePOp2.has_value()) {
      throw runtime_error("FPDBStoreFileScanPOp not found in pushdown subPlan");
    }

    // wire up local ops following the connection among store ops
    for (const auto &storeToLocalIt: storePOpToLocalPOp) {
      auto storePOp = storeToLocalIt.first;
      auto localPOp = storeToLocalIt.second;

      if (localPOp->getType() == POpType::CACHE_LOAD) {
        // already completed connected when created
        continue;
      } else {
        for (const auto &storeConsumerName: storePOp->consumers()) {
          auto storeConsumer = storePOpMap.find(storeConsumerName)->second;
          // need to get rid of CollatePOp here
          if (storeConsumer->getType() == POpType::COLLATE) {
            continue;
          }
          auto localConsumer = storePOpToLocalPOp.find(storeConsumer)->second;
          localPOp->produce(localConsumer);
        }
        for (const auto &storeProducerName: storePOp->producers()) {
          auto storeProducer = storePOpMap.find(storeProducerName)->second;
          auto localProducer = storePOpToLocalPOp.find(storeProducer)->second;

          // if the corresponding local producer is CacheLoadPOp, then it needs to be connected to mergePOp1
          if (localProducer->getType() == POpType::CACHE_LOAD) {
            (*mergePOp1)->produce(localPOp);
            localPOp->consume(*mergePOp1);
          } else {
            localPOp->consume(localProducer);
          }
        }
      }
    }

    // wire up for the second merge
    auto expStoreCollatePOp = typedFPDBStoreSuperPOp->getSubPlan()->getRootPOp();
    if (!expStoreCollatePOp.has_value()) {
      throw runtime_error(expStoreCollatePOp.error());
    }
    auto storeCollatePOp = *expStoreCollatePOp;

    if (storeCollatePOp->producers().size() > 1) {
      throw runtime_error("Currently pushdown only supports plan with serial operators");
    }
    auto storeLastPOpName = *storeCollatePOp->producers().begin();
    auto storeLastPOp = storePOpMap.find(storeLastPOpName)->second;
    auto localLastPOp = storePOpToLocalPOp.find(storeLastPOp)->second;

    (*mergePOp2)->setLeftProducer(localLastPOp);
    localLastPOp->produce(*mergePOp2);
    (*mergePOp2)->setRightProducer(fpdbStoreSuperPOp);
    fpdbStoreSuperPOp->produce(*mergePOp2);

    // rename store ops
    for (const auto &renameIt: storePOpRenames) {
      typedFPDBStoreSuperPOp->getSubPlan()->renamePOp(renameIt.first, renameIt.second);
    }

    // enable bitmap pushdown if required
    if (StoreTransformTraits::FPDBStoreStoreTransformTraits()->isBitmapPushdownEnabled()) {
      enableBitmapPushdown(storePOpToLocalPOp, typedFPDBStoreSuperPOp);
    }
  }

  return {connPOps, allPOps};
}

void PrePToFPDBStorePTransformer::enableBitmapPushdown(
        const unordered_map<shared_ptr<PhysicalOp>, shared_ptr<PhysicalOp>> &storePOpToLocalPOp,
        const shared_ptr<fpdb_store::FPDBStoreSuperPOp> &fpdbStoreSuperPOp) {
  for (const auto &storeToLocalIt: storePOpToLocalPOp) {
    auto storePOp = storeToLocalIt.first;
    auto localPOp = storeToLocalIt.second;

    switch (localPOp->getType()) {
      case CACHE_LOAD: {
        auto typedLocalPOp = static_pointer_cast<cache::CacheLoadPOp>(localPOp);
        typedLocalPOp->enableBitmapPushdown();
        break;
      }
      case FILTER: {
        auto typedLocalPOp = static_pointer_cast<filter::FilterPOp>(localPOp);
        auto typedStorePOp = static_pointer_cast<filter::FilterPOp>(storePOp);
        typedLocalPOp->enableBitmapPushdown(fpdbStoreSuperPOp->name(), typedStorePOp->name(), true,
                                            fpdbStoreSuperPOp->getHost(), fpdbStoreSuperPOp->getPort());
        typedStorePOp->enableBitmapPushdown(fpdbStoreSuperPOp->name(), typedLocalPOp->name(), false,
                                            fpdbStoreSuperPOp->getHost(), fpdbStoreSuperPOp->getPort());
        break;
      }
      default: {
        // noop
      }
    }
  }
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
    case HYBRID:
      // note: hybrid will transform pushdown-only plan to hybrid after the entire plan is transformed
      // so here both modes call the same function
      return transformFilterableScanPushdownOnly(filterableScanPrePOp, partitionPredicates);
    case CACHING_ONLY:
      return transformFilterableScanCachingOnly(filterableScanPrePOp, partitionPredicates);
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
    const auto &scanPOp = make_shared<file::RemoteFileScanPOp>(fmt::format("RemoteFileScan[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                               projPredColumnNames,
                                                               partitionId % numNodes_,
                                                               bucket,
                                                               object,
                                                               table->getFormat(),
                                                               table->getSchema(),
                                                               partition->getNumBytes(),
                                                               fpdbStoreConnector_->getHost(),
                                                               fpdbStoreConnector_->getFileServicePort());
    scanPOps.emplace_back(scanPOp);

    // filter
    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
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
    const auto &scanPOp = make_shared<fpdb_store::FPDBStoreFileScanPOp>(fmt::format("FPDBStoreFileScan[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                                        projPredColumnNames,
                                                                        partitionId % numNodes_,
                                                                        bucket,
                                                                        object,
                                                                        table->getFormat(),
                                                                        table->getSchema(),
                                                                        partition->getNumBytes());
    scanPOps.emplace_back(scanPOp);

    // filter
    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
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
PrePToFPDBStorePTransformer::transformFilterableScanCachingOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                      const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates) {
  vector<shared_ptr<PhysicalOp>> allPOps, selfConnDownPOps;
  const auto &table = filterableScanPrePOp->getTable();
  vector<string> projectColumnNames{filterableScanPrePOp->getProjectColumnNames().begin(),
                                    filterableScanPrePOp->getProjectColumnNames().end()};

  /**
   * For each partition, construct:
   * a CacheLoad, a RemoteFileScan, a Merge, a Filter if needed
   */
  uint partitionId = 0;
  for (const auto &partitionPredicateIt: partitionPredicates) {
    const auto &partition = static_pointer_cast<ObjStorePartition>(partitionPredicateIt.first);
    const auto &predicate = partitionPredicateIt.second;
    const auto &bucket = partition->getBucket();
    const auto &object = partition->getObject();
    pair<long, long> scanRange{0, partition->getNumBytes()};

    // project column names and its union with project column names
    vector<string> predicateColumnNames;
    if (predicate) {
      const auto predicateColumnNameSet = predicate->involvedColumnNames();
      predicateColumnNames.assign(predicateColumnNameSet.begin(), predicateColumnNameSet.end());
    }
    const auto &projPredColumnNames = union_(projectColumnNames, predicateColumnNames);

    // weighted segment keys
    vector<shared_ptr<SegmentKey>> weightedSegmentKeys;
    weightedSegmentKeys.reserve(projPredColumnNames.size());
    for (const auto &weightedColumnName: projPredColumnNames) {
      weightedSegmentKeys.emplace_back(
              SegmentKey::make(partition, weightedColumnName, SegmentRange::make(scanRange.first, scanRange.second)));
    }

    // cache load
    const auto cacheLoadPOp = make_shared<cache::CacheLoadPOp>(fmt::format("CacheLoad[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                               projectColumnNames,
                                                               partitionId % numNodes_,
                                                               predicateColumnNames,
                                                               std::vector<std::set<std::string>>{},    // not needed
                                                               projPredColumnNames,
                                                               partition,
                                                               scanRange.first,
                                                               scanRange.second,
                                                               fpdbStoreConnector_);
    allPOps.emplace_back(cacheLoadPOp);

    // remote file scan
    const auto &scanPOp = make_shared<file::RemoteFileScanPOp>(fmt::format("RemoteFileScan[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                               projPredColumnNames,
                                                               partitionId % numNodes_,
                                                               bucket,
                                                               object,
                                                               table->getFormat(),
                                                               table->getSchema(),
                                                               partition->getNumBytes(),
                                                               fpdbStoreConnector_->getHost(),
                                                               fpdbStoreConnector_->getFileServicePort(),
                                                               nullopt,
                                                               false,
                                                               true);
    allPOps.emplace_back(scanPOp);

    // merge
    const auto &mergePOp = make_shared<merge::MergePOp>(fmt::format("Merge[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                        projPredColumnNames,
                                                        partitionId % numNodes_);
    allPOps.emplace_back(mergePOp);

    // connect
    cacheLoadPOp->setHitOperator(mergePOp);
    cacheLoadPOp->setMissOperatorToCache(scanPOp);
    scanPOp->produce(mergePOp);
    scanPOp->consume(cacheLoadPOp);
    mergePOp->setLeftProducer(cacheLoadPOp);
    mergePOp->setRightProducer(scanPOp);

    // filter
    if (predicate) {
      const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}/{}", separableSuperPrePOp_->getId(), bucket, object),
                                                             projectColumnNames,
                                                             partitionId % numNodes_,
                                                             predicate,
                                                             table,
                                                             weightedSegmentKeys);
      allPOps.emplace_back(filterPOp);
      mergePOp->produce(filterPOp);
      filterPOp->consume(mergePOp);
      selfConnDownPOps.emplace_back(filterPOp);
    } else {
      selfConnDownPOps.emplace_back(mergePOp);
    }

    ++partitionId;
  }

  return make_pair(selfConnDownPOps, allPOps);
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
