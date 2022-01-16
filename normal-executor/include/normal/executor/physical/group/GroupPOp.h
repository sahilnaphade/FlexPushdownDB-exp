//
// Created by matt on 13/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H

#include <normal/executor/physical/group/GroupKernel2.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/aggregate/function/AggregateFunction.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <string>
#include <vector>
#include <memory>

using namespace normal::executor::message;
using namespace std;

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
  GroupPOp(const string &name,
           const vector<string> &groupColumnNames,
           const vector<shared_ptr<aggregate::AggregateFunction>> &aggregateFunctions,
           const vector<string> &projectColumnNames);
  GroupPOp() = default;
  GroupPOp(const GroupPOp&) = default;
  GroupPOp& operator=(const GroupPOp&) = default;

  void onReceive(const Envelope &msg) override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

  GroupKernel2 kernel2_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GroupPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("kernel2", op.kernel2_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPPOP_H
