//
// Created by matt on 29/4/20.
//

#include <normal/executor/physical/hashjoin/HashJoinBuildPOp.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/tuple/TupleSet.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;
using namespace normal::tuple;

HashJoinBuildPOp::HashJoinBuildPOp(const string &name,
                                   const vector<string> &columnNames,
                                   const vector<string> &projectColumnNames) :
	PhysicalOp(name, "HashJoinBuildPOp", projectColumnNames),
	kernel_(HashJoinBuildKernel2::make(columnNames)){
}

void HashJoinBuildPOp::onReceive(const Envelope &msg) {
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
	throw runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
  }
}

void HashJoinBuildPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinBuildPOp::onTuple(const TupleMessage &msg) {
  const auto& tupleSet = msg.tuples();

  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));

  auto result = buffer(tupleSet);
  if(!result)
    throw runtime_error(fmt::format("{}, {}", result.error(), name()));
  send(false);
}

void HashJoinBuildPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    send(true);
    ctx()->notifyComplete();
  }
}

tl::expected<void, string> HashJoinBuildPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
  return kernel_.put(tupleSet);
}

void HashJoinBuildPOp::send(bool force) {
  if (kernel_.getTupleSetIndex().has_value() && (force || kernel_.getTupleSetIndex().value()->size() >= DefaultBufferSize)) {
    shared_ptr<Message> message =
            make_shared<TupleSetIndexMessage>(kernel_.getTupleSetIndex().value(), name());
    ctx()->tell(message);
    kernel_.clear();
  }
}
