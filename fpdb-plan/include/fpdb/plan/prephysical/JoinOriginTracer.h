//
// Created by Yifei Yang on 4/14/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H

#include <fpdb/plan/prephysical/PrePhysicalPlan.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/SortPrePOp.h>
#include <fpdb/plan/prephysical/LimitSortPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/prephysical/NestedLoopJoinPrePOp.h>

namespace fpdb::plan::prephysical {

struct JoinOrigin {
  JoinOrigin(const std::shared_ptr<FilterableScanPrePOp> &left, const std::shared_ptr<FilterableScanPrePOp> &right):
    left_(left), right_(right) {}

  std::shared_ptr<FilterableScanPrePOp> left_;
  std::shared_ptr<FilterableScanPrePOp> right_;
};

class JoinOriginTracer {

public:
  static std::vector<std::shared_ptr<JoinOrigin>> trace(const std::shared_ptr<PrePhysicalPlan> &plan);

private:
  struct ColumnOrigin {
    ColumnOrigin(const std::string &name):
      name_(name), currName_(name) {}

    std::string name_;      // original name from the join op
    std::string currName_;  // there may be some project ops that rename columns, so need to keep track of the current
    std::shared_ptr<FilterableScanPrePOp> originOp_ = nullptr;
  };

  JoinOriginTracer(const std::shared_ptr<PrePhysicalPlan> &plan);

  const std::vector<std::shared_ptr<JoinOrigin>> &getJoinOrigins() const;

  void trace();
  void traceDFS(const std::shared_ptr<PrePhysicalOp> &op,
                const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceAggregate(const std::shared_ptr<AggregatePrePOp> &op,
                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceGroup(const std::shared_ptr<GroupPrePOp> &op,
                  const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceSort(const std::shared_ptr<SortPrePOp> &op,
                 const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceLimitSort(const std::shared_ptr<LimitSortPrePOp> &op,
                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceFilter(const std::shared_ptr<FilterPrePOp> &op,
                   const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceFilterableScan(const std::shared_ptr<FilterableScanPrePOp> &op,
                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceProject(const std::shared_ptr<ProjectPrePOp> &op,
                    const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceHashJoin(const std::shared_ptr<HashJoinPrePOp> &op,
                     const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  void traceNestedLoopJoin(const std::shared_ptr<NestedLoopJoinPrePOp> &op,
                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);

  std::shared_ptr<PrePhysicalPlan> plan_;
  std::vector<std::shared_ptr<JoinOrigin>> joinOrigins_;
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H
