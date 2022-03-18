//
// Created by matt on 13/5/20.
//

#include <fpdb/executor/physical/group/GroupPOp.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::group {

GroupPOp::GroupPOp(const string &name,
                   const vector<string> &projectColumnNames,
                   int nodeId,
                   const vector<string> &groupColumnNames,
                   const vector<shared_ptr<aggregate::AggregateFunction>> &aggregateFunctions) :
	PhysicalOp(name, GROUP, projectColumnNames, nodeId),
  kernel_(groupColumnNames, aggregateFunctions) {
}

std::string GroupPOp::getTypeString() const {
  return "GroupPOp";
}

void GroupPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.message().getTypeString());
  }
}

void GroupPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GroupPOp::onTupleSet(const TupleSetMessage &message) {
  const auto &tupleSet = message.tuples();
  auto expectedGroupResult = kernel_.group(*tupleSet);
  if(!expectedGroupResult)
    ctx()->notifyError(expectedGroupResult.error());
}

void GroupPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {

    auto expectedGroupedTupleSet = kernel_.finalise();
    if (!expectedGroupedTupleSet)
      ctx()->notifyError(expectedGroupedTupleSet.error());

    // Project using projectColumnNames
    auto expProjectTupleSet = expectedGroupedTupleSet.value()->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      ctx()->notifyError(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), this->name());
    ctx()->tell(tupleSetMessage);

    ctx()->notifyComplete();
  }
}

void GroupPOp::clear() {
  kernel_.clear();
}

}
