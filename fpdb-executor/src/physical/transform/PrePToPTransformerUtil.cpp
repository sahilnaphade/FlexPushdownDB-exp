//
// Created by Yifei Yang on 2/22/22.
//

#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/executor/physical/aggregate/function/MinMax.h>
#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/expression/gandiva/Column.h>
#include <queue>

namespace fpdb::executor::physical {
  
void PrePToPTransformerUtil::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                           vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw runtime_error(fmt::format("Bad one-to-one operator connection input, producers has {}, but consumers has {}",
                                    producers.size(), consumers.size()));
  }
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

void PrePToPTransformerUtil::connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                             vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &producer: producers) {
    for (const auto &consumer: consumers) {
      producer->produce(consumer);
      consumer->consume(producer);
    }
  }
}

void PrePToPTransformerUtil::connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                            shared_ptr<PhysicalOp> &consumer) {
  for (const auto &producer: producers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}

vector<shared_ptr<aggregate::AggregateFunction>>
PrePToPTransformerUtil::transformAggFunction(const string &outputColumnName,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction,
                                         bool hasReduceOp) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return {make_shared<aggregate::Sum>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::COUNT: {
      return {make_shared<aggregate::Count>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MIN: {
      return {make_shared<aggregate::MinMax>(true, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MAX: {
      return {make_shared<aggregate::MinMax>(false, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::AVG: {
      if (hasReduceOp) {
        auto sumFunc = make_shared<aggregate::Sum>(
                AggregatePrePFunction::AVG_PARALLEL_SUM_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        auto countFunc = make_shared<aggregate::Count>(
                AggregatePrePFunction::AVG_PARALLEL_COUNT_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        return {sumFunc, countFunc};
      } else {
        return {make_shared<aggregate::Avg>(outputColumnName, prePFunction->getExpression())};
      }
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregateFunction>
PrePToPTransformerUtil::transformAggReduceFunction(const string &outputColumnName,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM:
    case plan::prephysical::COUNT: {
      return make_shared<aggregate::Sum>(outputColumnName,
                                         fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MIN: {
      return make_shared<aggregate::MinMax>(true,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MAX: {
      return make_shared<aggregate::MinMax>(false,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::AVG: {
      return make_shared<aggregate::AvgReduce>(outputColumnName,nullptr);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type for parallel execution: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<PhysicalPlan> PrePToPTransformerUtil::rootOpToPlan(const shared_ptr<PhysicalOp> &rootOp,
                                                              const unordered_map<string, shared_ptr<PhysicalOp>> &opMap) {
  // collect operators in the subtree of the rootOp
  vector<shared_ptr<PhysicalOp>> ops{rootOp};
  queue<shared_ptr<PhysicalOp>> pendOpQueue;
  pendOpQueue.push(rootOp);

  while (!pendOpQueue.empty()) {
    auto op = pendOpQueue.front();
    for (const auto &producerName: op->producers()) {
      auto producerIt = opMap.find(producerName);
      if (producerIt == opMap.end()) {
        throw runtime_error(fmt::format("Error when making physical plan from root op: producer {} not found in opMap", producerName));
      }
      auto producer = producerIt->second;
      ops.emplace_back(producer);
      pendOpQueue.push(producer);
    }
    pendOpQueue.pop();
  }

  return make_shared<PhysicalPlan>(ops);
}
  
}
