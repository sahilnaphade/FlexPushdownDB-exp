//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H

#include <normal/executor/physical/aggregate/AggregationResult.h>
#include <normal/expression/Projector.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/TupleSet.h>
#include <memory>

using namespace normal::tuple;

namespace normal::executor::physical::aggregate {

/**
 * Base class for aggregation functions
 */
class AggregationFunction {

protected:

  /**
   * The expression projector, created and cached when input schema is extracted from first tuple received
   */
  std::optional<std::shared_ptr<normal::expression::Projector>> projector_;

  /**
   * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
   * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
   */
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

  /**
   * Field name of aggregate result
   */
  std::string alias_;

  /**
   * Compute Expression for this aggregate function, like "A*B" in sum(A * B)
   */
  std::shared_ptr<normal::expression::gandiva::Expression> expression_;

public:
  explicit AggregationFunction(std::string alias,
                               std::shared_ptr<normal::expression::gandiva::Expression> expression);
  virtual ~AggregationFunction() = default;

  /**
   * Alias is the symbolic name of the attribute, it's not guaranteed to be unique so shouldn't be used for anything
   * important. Ostensibly just for labelling attributes in query output.
   *
   * FIXME: Support this being undefined, perhaps it should be an Optional?
   *
   * @return
   */
  const std::string &getAlias() const;
  const std::shared_ptr<normal::expression::gandiva::Expression> &getExpression() const;

  virtual void apply(std::shared_ptr<aggregate::AggregationResult> result, std::shared_ptr<TupleSet> tuples) = 0;
  virtual std::shared_ptr<arrow::DataType> returnType() = 0;

  /**
   * Invoked when an aggregate function should expect no more data to give it an opportunity to
   * compute its final result.
   */
  virtual void finalize(std::shared_ptr<aggregate::AggregationResult> result) = 0;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEFUNCTION_H
