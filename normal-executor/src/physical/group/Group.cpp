//
// Created by matt on 13/5/20.
//

#include <normal/executor/physical/group/Group.h>

using namespace normal::tuple;

namespace normal::executor::physical::group {

Group::Group(const std::string &Name,
			 const std::vector<std::string> &GroupColumnNames,
			 const std::vector<std::string> &AggregateColumnNames,
			 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
			 long queryId) :
	PhysicalOp(Name, "Group", queryId),
  kernel2_(std::make_unique<GroupKernel2>(GroupColumnNames, AggregateColumnNames, *AggregateFunctions)) {
}

std::shared_ptr<Group> Group::make(const std::string &Name,
								   const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::string> &aggregateColumnNames,
								   const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
								   long queryId) {

  return std::make_shared<Group>(Name, groupColumnNames, aggregateColumnNames, AggregateFunctions, queryId);
}

void Group::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void Group::onStart() {
  SPDLOG_DEBUG("Starting");
}

void Group::onTuple(const TupleMessage &message) {
  auto tupleSet = normal::tuple::TupleSet2::create(message.tuples());
  auto expectedGroupResult = kernel2_->group(*tupleSet);
  if(!expectedGroupResult)
    throw std::runtime_error(expectedGroupResult.error());
}

void Group::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {

    if (kernel2_->hasInput()) {
      auto expectedGroupedTupleSet = kernel2_->finalise();
      if (!expectedGroupedTupleSet)
        throw std::runtime_error(expectedGroupedTupleSet.error());

      std::shared_ptr<Message>
              tupleMessage =
              std::make_shared<TupleMessage>(expectedGroupedTupleSet.value()->toTupleSetV1(),
                                                                    this->name());
      ctx()->tell(tupleMessage);
    }

    ctx()->notifyComplete();
  }
}

}
