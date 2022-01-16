//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H

#include <normal/executor/physical/aggregate/AggregateResult.h>
#include <normal/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <normal/expression/Projector.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/TupleSet.h>
#include <memory>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::aggregate {

/**
 * Base class for aggregate functions
 */
class AggregateFunction {

public:
  explicit AggregateFunction(AggregateFunctionType type,
                             string outputColumnName,
                             shared_ptr<normal::expression::gandiva::Expression> expression);
  AggregateFunction() = default;
  AggregateFunction(const AggregateFunction&) = default;
  AggregateFunction& operator=(const AggregateFunction&) = default;
  virtual ~AggregateFunction() = default;

  AggregateFunctionType getType() const;
  const string &getOutputColumnName() const;
  const shared_ptr<normal::expression::gandiva::Expression> &getExpression() const;
  virtual shared_ptr<arrow::DataType> returnType() = 0;
  virtual set<string> involvedColumnNames() = 0;

  /**
   * Perform the aggregation given an input tupleSet.
   * @param tupleSet
   */
  virtual tl::expected<shared_ptr<AggregateResult>, string>
  compute(const shared_ptr<TupleSet> &tupleSet) = 0;

  /**
   * Finalize aggregate results, e.g. compute mean from a vector of sum and count values.
   * @return
   */
  virtual tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) = 0;

  void compile(const shared_ptr<arrow::Schema> &schema);

protected:
  /**
   * Used to evaluate input tupleSet using expression_
   */
  shared_ptr<arrow::ChunkedArray> evaluateExpr(const shared_ptr<TupleSet> &tupleSet);
  void cacheInputSchema(const shared_ptr<arrow::Schema> &schema);
  void buildAndCacheProjector();

  /**
   * Build input array for finalize()
   */
  tl::expected<shared_ptr<arrow::Array>, string>
  buildFinalizeInputArray(const vector<shared_ptr<AggregateResult>> &aggregateResults, const string &key);

  /*
   * Aggregate function type
   */
  AggregateFunctionType type_;

  /**
   * Field name of aggregate result
   */
  string outputColumnName_;

  /**
   * Compute Expression for this aggregate function, like "A*B" in sum(A * B)
   */
  shared_ptr<normal::expression::gandiva::Expression> expression_;

  /**
   * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
   * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
   */
  optional<shared_ptr<arrow::Schema>> inputSchema_;
  
  /**
   * The expression projector, created and cached when input schema is extracted from first tuple received
   */
  optional<shared_ptr<normal::expression::Projector>> projector_;

  /**
   * Data type of aggregate output column
   */
  shared_ptr<arrow::DataType> returnType_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H
