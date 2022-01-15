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
               vector<shared_ptr<AggregateFunction>> functions,
               vector<string> projectColumnNames);
  AggregatePOp() = default;
  AggregatePOp(const AggregatePOp&) = default;
  AggregatePOp& operator=(const AggregatePOp&) = default;
  ~AggregatePOp() override = default;

  void onReceive(const Envelope &message) override;

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
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
