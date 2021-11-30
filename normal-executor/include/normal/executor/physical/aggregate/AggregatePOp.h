//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H

#include <normal/executor/physical/aggregate/AggregationResult.h>
#include <normal/executor/physical/aggregate/AggregationFunction.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <memory>
#include <string>
#include <vector>

namespace normal::executor::physical::aggregate {

class AggregatePOp : public normal::executor::physical::PhysicalOp {

public:
  AggregatePOp(std::string name,
            std::vector<std::string> projectColumnNames,
            std::vector<std::shared_ptr<aggregate::AggregationFunction>> functions);
  ~AggregatePOp() override = default;

  void onReceive(const normal::executor::message::Envelope &message) override;
  void compute(const std::shared_ptr<TupleSet> &tuples);
  void cacheInputSchema(const normal::executor::message::TupleMessage &message);

private:
  std::vector<std::shared_ptr<aggregate::AggregationFunction>> functions_;
  std::vector<std::shared_ptr<aggregate::AggregationResult>> results_;

  /**
   * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
   * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
   */
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

  void onStart();
  void onTuple(const normal::executor::message::TupleMessage &message);
  void onComplete(const normal::executor::message::CompleteMessage &message);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
