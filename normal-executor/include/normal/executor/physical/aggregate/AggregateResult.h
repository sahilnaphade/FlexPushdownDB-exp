//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H

#include <normal/tuple/TupleSet.h>
#include <normal/tuple/Scalar.h>
#include <string>
#include <unordered_map>

using namespace std;

namespace normal::executor::physical::aggregate {

/**
 * Structure for aggregate functions to store intermediate results.
 *
 * It is intended for there to be one of these per aggregate function.
 *
 * This is roughly equivalent to a map, so aggregate functions can store multiple values, before
 * computing the final result. E.g. Mean most accurately calculated by storing count and sum before computing
 * the final result.
 */
class AggregateResult {

public:
  AggregateResult() = default;
  AggregateResult(const AggregateResult&) = default;
  AggregateResult& operator=(const AggregateResult&) = default;

  void put(const string &key, const shared_ptr<arrow::Scalar> &value);
  optional<shared_ptr<arrow::Scalar>> get(const string &key);

private:
  // use normal::tuple::Scalar instead of arrow::Scalar for ease of serialization
  unordered_map<string, shared_ptr<normal::tuple::Scalar>> resultMap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AggregateResult& res) {
    return f.apply(res.resultMap_);
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H
