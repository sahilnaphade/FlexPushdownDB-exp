//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/plan/prephysical/PrePhysicalPlan.h>
#include <fpdb/plan/prephysical/SortPrePOp.h>
#include <fpdb/plan/prephysical/LimitSortPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/prephysical/NestedLoopJoinPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/CatalogueEntry.h>
#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::catalogue;
using namespace fpdb::catalogue::obj_store;

namespace fpdb::executor::physical {

class PrePToPTransformer {
public:
  PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                     const shared_ptr<CatalogueEntry> &catalogueEntry,
                     const shared_ptr<ObjStoreConnector> &objStoreConnector,
                     const shared_ptr<Mode> &mode,
                     int parallelDegree,
                     int numNodes);

  shared_ptr<PhysicalPlan> transform();

private:
  /**
   * Transform prephysical op to physical op in dfs style
   * @param prePOp: prephysical op
   * @return a pair of connect physical ops (to producer) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformDfs(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
  transformProducers(const shared_ptr<PrePhysicalOp> &prePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformSort(const shared_ptr<SortPrePOp> &sortPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp);

  shared_ptr<PrePhysicalPlan> prePhysicalPlan_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
  shared_ptr<ObjStoreConnector> objStoreConnector_;
  shared_ptr<Mode> mode_;
  int parallelDegree_;
  int numNodes_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H
