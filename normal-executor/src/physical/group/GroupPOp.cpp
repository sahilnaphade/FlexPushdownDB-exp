//
// Created by matt on 13/5/20.
//

#include <normal/executor/physical/group/GroupPOp.h>

using namespace normal::tuple;

namespace normal::executor::physical::group {

GroupPOp::GroupPOp(const std::string &name,
			 const std::vector<std::string> &groupColumnNames,
			 const std::vector<std::shared_ptr<aggregate::AggregationFunction>> &aggregateFunctions,
       const std::vector<std::string> &projectColumnNames,
			 long queryId) :
	PhysicalOp(name, "GroupPOp", projectColumnNames, queryId),
  kernel2_(std::make_unique<GroupKernel2>(groupColumnNames, aggregateFunctions)) {
}

void GroupPOp::onReceive(const Envelope &msg) {
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

void GroupPOp::onStart() {
  SPDLOG_DEBUG("Starting");
}

void GroupPOp::onTuple(const TupleMessage &message) {
  const auto &tupleSet = message.tuples();
  auto expectedGroupResult = kernel2_->group(*tupleSet);
  if(!expectedGroupResult)
    throw std::runtime_error(expectedGroupResult.error());
}

void GroupPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {

    if (kernel2_->hasInput()) {
      auto expectedGroupedTupleSet = kernel2_->finalise();
      if (!expectedGroupedTupleSet)
        throw std::runtime_error(expectedGroupedTupleSet.error());

      std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(expectedGroupedTupleSet.value(),
                                                                             this->name());
      ctx()->tell(tupleMessage);
    }

    ctx()->notifyComplete();
  }
}

}
