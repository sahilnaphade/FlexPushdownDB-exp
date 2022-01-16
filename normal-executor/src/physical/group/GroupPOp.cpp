//
// Created by matt on 13/5/20.
//

#include <normal/executor/physical/group/GroupPOp.h>

using namespace normal::tuple;

namespace normal::executor::physical::group {

GroupPOp::GroupPOp(const string &name,
                   const vector<string> &groupColumnNames,
                   const vector<shared_ptr<aggregate::AggregateFunction>> &aggregateFunctions,
                   const vector<string> &projectColumnNames) :
	PhysicalOp(name, "GroupPOp", projectColumnNames),
  kernel2_(groupColumnNames, aggregateFunctions) {
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
	throw runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void GroupPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GroupPOp::onTuple(const TupleMessage &message) {
  const auto &tupleSet = message.tuples();
  auto expectedGroupResult = kernel2_.group(*tupleSet);
  if(!expectedGroupResult)
    throw runtime_error(expectedGroupResult.error());
}

void GroupPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {

    auto expectedGroupedTupleSet = kernel2_.finalise();
    if (!expectedGroupedTupleSet)
      throw runtime_error(expectedGroupedTupleSet.error());

    // Project using projectColumnNames
    auto expProjectTupleSet = expectedGroupedTupleSet.value()->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      throw runtime_error(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
    ctx()->tell(tupleMessage);

    ctx()->notifyComplete();
  }
}

}
