//
// Created by Yifei Yang on 3/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fpdb/expression/gandiva/Expression.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::catalogue::obj_store;
using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical {

class PrePToFPDBStorePTransformer {

public:
  /**
   * Transform separable super prephysical op to physical op
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transform(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
            const shared_ptr<Mode> &mode,
            int numNodes,
            const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector);

  /**
   * Add each of the separable operators to the corresponding FPDBStoreSuperPOp
   * if the producers is FPDBStoreSuperPOp and the operator type is enabled for pushdown
   * @param producers
   * @param bloomFilterUsePOps
   * @return a pair of connect physical ops (to consumers) and additional physical ops to add to plan
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  addSeparablePOp(vector<shared_ptr<PhysicalOp>> &producers,
                  vector<shared_ptr<PhysicalOp>> &separablePOps,
                  const shared_ptr<Mode> &mode);

private:
  PrePToFPDBStorePTransformer(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
                              const shared_ptr<Mode> &mode,
                              int numNodes,
                              const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector);
  
  /**
   * Impl of transformation
   * @return 
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>> transform();
  
  /**
   * Transform prephysical op to physical op in dfs style
   * @param prePOp: prephysical op
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformDfs(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
  transformProducers(const shared_ptr<PrePhysicalOp> &prePOp);

  /**
   * Transform the plan of pushdown-only into hybrid execution, where the plan of pushdown-only is just FpdbStoreSuperPOps
   * @param fpdbStoreSuperPOps
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformPushdownOnlyToHybrid(const vector<shared_ptr<PhysicalOp>> &fpdbStoreSuperPOps);

  void enableBitmapPushdown(const unordered_map<shared_ptr<PhysicalOp>, shared_ptr<PhysicalOp>> &storePOpToLocalPOp,
                            const shared_ptr<fpdb_store::FPDBStoreSuperPOp> &fpdbStoreSuperPOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPushdownOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                      const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanCachingOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                     const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  shared_ptr<SeparableSuperPrePOp> separableSuperPrePOp_;
  std::shared_ptr<Mode> mode_;
  int numNodes_;
  std::shared_ptr<FPDBStoreConnector> fpdbStoreConnector_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
