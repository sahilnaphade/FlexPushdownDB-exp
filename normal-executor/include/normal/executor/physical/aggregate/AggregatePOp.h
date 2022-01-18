//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>
#include <normal/executor/physical/aggregate/AggregateResult.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <memory>
#include <string>
#include <vector>

using namespace normal::executor::message;

namespace normal::executor::physical::aggregate {

class AggregatePOp : public normal::executor::physical::PhysicalOp {

public:
  AggregatePOp(string name,
               vector<string> projectColumnNames,
               int nodeId,
               vector<shared_ptr<AggregateFunction>> functions);
  AggregatePOp() = default;
  AggregatePOp(const AggregatePOp&) = default;
  AggregatePOp& operator=(const AggregatePOp&) = default;
  ~AggregatePOp() override = default;

  void onReceive(const Envelope &message) override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onTuple(const TupleMessage &message);
  void onComplete(const CompleteMessage &message);

  void compute(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> finalize();
  shared_ptr<TupleSet> finalizeEmpty();

  bool hasResult();
  
  vector<shared_ptr<AggregateFunction>> functions_;
  vector<vector<shared_ptr<AggregateResult>>> aggregateResults_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AggregatePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("functions", op.functions_),
                               f.field("aggregateResults", op.aggregateResults_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
