//
// Created by matt on 11/12/19.
//

#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/aggregate/AggregationResult.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/Message.h>
#include <arrow/scalar.h>
#include <string>
#include <utility>
#include <memory>

namespace normal::executor::physical::aggregate {

AggregatePOp::AggregatePOp(std::string name,
                     std::vector<std::string> projectColumnNames,
                     std::vector<std::shared_ptr<aggregate::AggregationFunction>> functions,
                     long queryId)
    : PhysicalOp(std::move(name), "Aggregate", std::move(projectColumnNames), queryId),
      functions_(std::move(functions)) {}

void AggregatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  for(size_t i=0;i<functions_.size();++i) {
	auto result = std::make_shared<aggregate::AggregationResult>();
	results_.emplace_back(result);
  }
}

void AggregatePOp::onReceive(const normal::executor::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::executor::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::executor::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void AggregatePOp::onComplete(const normal::executor::message::CompleteMessage &) {
  if (!ctx()->isComplete() &&
    this->ctx()->operatorMap().allComplete(normal::executor::physical::POpRelationshipType::Producer)) {

    // Create output schema
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto &function: functions_) {
      std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), function->returnType());
      fields.emplace_back(field);
    }
    schema = arrow::schema(fields);

    // Create output tuples
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for(size_t i=0;i<functions_.size();++i){
      auto function = functions_.at(i);
      auto result = results_.at(i);

      function->finalize(result);

      if(function->returnType() == arrow::float64()){
        auto scalar = std::static_pointer_cast<arrow::DoubleScalar>(result->evaluate());
        auto colArgh = makeArgh<arrow::DoubleType>(scalar);
        columns.emplace_back(colArgh.value());
      }
      else if(function->returnType() == arrow::int32()){
        auto scalar = std::static_pointer_cast<arrow::Int32Scalar>(result->evaluate());
        auto colArgh = makeArgh<arrow::Int32Type>(scalar);
        columns.emplace_back(colArgh.value());
      } else if(function->returnType() == arrow::int64()){
        auto scalar = std::static_pointer_cast<arrow::Int64Scalar>(result->evaluate());
        auto colArgh = makeArgh<arrow::Int64Type>(scalar);
        columns.emplace_back(colArgh.value());
      }
      else{
        throw std::runtime_error("Unrecognized aggregation type " + function->returnType()->name());
      }
    }

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, columns);

    const std::shared_ptr<TupleSet> &aggregatedTuples = TupleSet::make(table);

    SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}", aggregatedTuples->toString());

    std::shared_ptr<normal::executor::message::Message>
        tupleMessage = std::make_shared<normal::executor::message::TupleMessage>(aggregatedTuples, this->name());
    ctx()->tell(tupleMessage);

    ctx()->notifyComplete();
  }
}

void AggregatePOp::onTuple(const normal::executor::message::TupleMessage &message) {
  // Set the input schema if not yet set
  cacheInputSchema(message);
  compute(message.tuples());
}

void AggregatePOp::cacheInputSchema(const normal::executor::message::TupleMessage &message) {
  if(!inputSchema_.has_value()){
	inputSchema_ = message.tuples()->table()->schema();
  }
}

void AggregatePOp::compute(const std::shared_ptr<TupleSet> &tuples) {
  for(size_t i=0;i<functions_.size();++i){
    auto function = functions_.at(i);
    auto result = results_.at(i);
	function->apply(result, tuples);
  }
}

}