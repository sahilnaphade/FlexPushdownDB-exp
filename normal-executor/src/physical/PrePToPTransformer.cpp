//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/PrePToPTransformer.h>
#include <normal/executor/physical/sort/SortPOp.h>
#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <fmt/format.h>
#include <cassert>

namespace normal::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<Mode> &mode) :
  prePhysicalPlan_(prePhysicalPlan),
  mode_(mode) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // TODO
  return shared_ptr<PhysicalPlan>();
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  switch (prePOp->getType()) {
    case Sort: {
      const auto &sortPrePOp = std::static_pointer_cast<SortPrePOp>(prePOp);
      return transformSort(sortPrePOp);
    }
    case FilterableScan:
    case Filter:
    case HashJoin:
    case Aggregate:
    case Group:
    case Project:
    default: {
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
PrePToPTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformSort(const shared_ptr<SortPrePOp> &sortPrePOp) {
  // transform producers
  const auto &producersTransRes = transformProducers(sortPrePOp);
  assert(producersTransRes.size() == 1);

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<sort::SortPOp>(fmt::format("sort-{}", index),
                                                     projectColumnNames,
                                                     queryId_));
  }

  // connect
  connectOneToOne(upConnPOps, selfPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  assert(producersTransRes.size() == 1);

  // aggregate functions
  vector<shared_ptr<aggregate::AggregationFunction>> aggFunctions, aggReduceFunctions;
  for (size_t i = 0; i < aggregatePrePOp->getFunctions().size(); ++i) {
    const auto &prepFunction = aggregatePrePOp->getFunctions()[i];
    const auto alias = aggregatePrePOp->getAggOutputColumnNames()[i];
    aggFunctions.emplace_back(transformAggFunction(alias, prepFunction));
    aggReduceFunctions.emplace_back(transformAggReduceFunction(alias, prepFunction));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  for (size_t i = 0; i < upConnPOps.size(); ++i) {
    selfPOps.emplace_back(make_shared<aggregate::AggregatePOp>(fmt::format("aggregate-{}", index),
                                                               projectColumnNames,
                                                               aggFunctions,
                                                               queryId_));
  }

  // if num > 1, then we need a aggregate reduce operator
  if (upConnPOps.size() == 1) {
    selfConnUpPOps = selfPOps;
    selfConnDownPOps = selfPOps;
  } else {
    auto aggReducePOp = make_shared<aggregate::AggregatePOp>("aggregate-reduce",
                                                             projectColumnNames,
                                                             aggReduceFunctions,
                                                             queryId_);
    for (const auto &aggPOp: selfPOps) {
      aggPOp->produce(aggReducePOp);
      aggReducePOp->consume(aggPOp);
    }
    selfConnUpPOps = selfPOps;
    selfConnDownPOps.emplace_back(aggReducePOp);
    selfPOps.emplace_back(aggReducePOp);
  }

  // connect
  connectOneToOne(upConnPOps, selfConnUpPOps);

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfConnDownPOps, allPOps);

}

shared_ptr<aggregate::AggregationFunction>
PrePToPTransformer::transformAggFunction(const string &alias,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(alias, prePFunction->getExpression());
    }
    case plan::prephysical::COUNT:
    case plan::prephysical::MIN:
    case plan::prephysical::MAX:
    case plan::prephysical::AVG:
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregationFunction>
PrePToPTransformer::transformAggReduceFunction(const string &alias,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return make_shared<aggregate::Sum>(alias, normal::expression::gandiva::col(alias));
    }
    case plan::prephysical::COUNT:
    case plan::prephysical::MIN:
    case plan::prephysical::MAX:
    case plan::prephysical::AVG:
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

void PrePToPTransformer::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                         vector<shared_ptr<PhysicalOp>> &consumers) {
  assert(producers.size() == consumers.size());
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

}
