//
// Created by matt on 13/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H

#include <normal/executor/physical/group/GroupKernel2.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <string>
#include <vector>
#include <memory>

using namespace normal::executor::message;

namespace normal::executor::physical::group {

/**
 * Group with aggregate operator
 *
 * This operator takes a list of column names to group by and a list of aggregate expressions to
 * compute for each group.
 *
 * A map is created from grouped values to computed aggregates. On the receipt of each tuple set
 * the aggregates are computed for each group and stored in the map.
 *
 * Upon completion of all upstream operators the final aggregates for each group are sent to downstream operators.
 */
class GroupPOp : public PhysicalOp {

public:
  GroupPOp(const std::string &Name,
		const std::vector<std::string> &ColumnNames,
    const std::vector<std::string> &AggregateColumnNames,
		const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
		long queryId = 0);

  static std::shared_ptr<GroupPOp> make(const std::string &Name,
									 const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::string> &aggregateColumnNames,
									 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
									 long queryId);

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

  std::unique_ptr<GroupKernel2> kernel2_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H
