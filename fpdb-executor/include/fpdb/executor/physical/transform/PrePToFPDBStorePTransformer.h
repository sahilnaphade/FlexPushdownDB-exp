//
// Created by Yifei Yang on 3/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/expression/gandiva/Expression.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical {

class PrePToFPDBStorePTransformer {

public:
  PrePToFPDBStorePTransformer(uint prePOpId,
                              const shared_ptr<Mode> &mode,
                              int numNodes,
                              const std::string &host,
                              int fileServicePort,
                              int flightPort);

  /**
   * Transform separable super prephysical op to physical op
   * @param separableSuperPrePOp
   * @return a pair of connect physical ops (to producer) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp);

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
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPushdownOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                      const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  uint prePOpId_;
  std::shared_ptr<Mode> mode_;
  int numNodes_;
  std::string host_;
  int fileServicePort_;
  int flightPort_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
