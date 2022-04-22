//
// Created by Yifei Yang on 4/20/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H

#include <fpdb/executor/physical/group/GroupAbstractKernel.h>
#include <arrow/compute/exec/options.h>

namespace fpdb::executor::physical::group {

class GroupArrowKernel: public GroupAbstractKernel{

public:
  GroupArrowKernel(const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions);

  tl::expected<void, std::string> group(const std::shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<std::shared_ptr<TupleSet>, std::string> finalise() override;
  void clear() override;

private:
  static constexpr std::string_view AGGREGATE_COLUMN_PREFIX = "AGG_COLUMN_";

  tl::expected<std::shared_ptr<TupleSet>, std::string> evaluateExpr(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, std::string> prepareGroup(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<std::shared_ptr<TupleSet>, std::string> doGroup(const std::shared_ptr<TupleSet> &tupleSet);
  std::string getAggregateColumnName(int functionId);

  std::optional<std::shared_ptr<arrow::Schema>> outputSchema_;
  std::optional<arrow::compute::AggregateNodeOptions> aggregateNodeOptions_;
  std::vector<std::shared_ptr<TupleSet>> groupResults_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H
