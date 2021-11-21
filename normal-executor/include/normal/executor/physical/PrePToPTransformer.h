//
// Created by Yifei Yang on 11/20/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PREPTOPTRANSFORMER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PREPTOPTRANSFORMER_H

#include <normal/executor/physical/PhysicalPlan.h>
#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <normal/plan/prephysical/PrePhysicalPlan.h>
#include <normal/plan/prephysical/SortPrePOp.h>
#include <normal/plan/prephysical/AggregatePrePOp.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>
#include <normal/plan/Mode.h>

using namespace normal::plan;
using namespace normal::plan::prephysical;

namespace normal::executor::physical {

class PrePToPTransformer {
public:
  PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                     const shared_ptr<Mode> &mode);

  shared_ptr<PhysicalPlan> transform();

private:
  /**
   * Transform prephysical op to physical op in dfs style
   * @param prePOp: prephysical op
   * @param upConnPOps: physical ops from upstream to connect, can be multiple groups of ops
   * @param selfConnPOps: physical ops from self to connect, can be multiple groups of ops
   * @return a pair of connect physical ops (to producer) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformDfs(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
  transformProducers(const shared_ptr<PrePhysicalOp> &prePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformSort(const shared_ptr<SortPrePOp> &sortPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  /**
   * Transform aggregate and aggregate reduce function
   * @param alias
   * @param prePFunction
   * @return
   */
  shared_ptr<aggregate::AggregationFunction> transformAggFunction(const string &alias,
                                                                  const shared_ptr<AggregatePrePFunction> &prePFunction);
  shared_ptr<aggregate::AggregationFunction> transformAggReduceFunction(const string &alias,
                                                                        const shared_ptr<AggregatePrePFunction> &prePFunction);

  /**
   * One-to-one connection between producers of consumers
   * @param producers
   * @param consumers
   */
  void connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                       vector<shared_ptr<PhysicalOp>> &consumers);

  shared_ptr<PrePhysicalPlan> prePhysicalPlan_;
  shared_ptr<Mode> mode_;
  long queryId_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PREPTOPTRANSFORMER_H
