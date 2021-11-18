//
// Created by matt on 29/4/20.
//

#include <normal/executor/physical/hashjoin/HashJoinProbePOp.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/tuple/TupleSetIndexWrapper.h>
#include <normal/tuple/arrow/SchemaHelper.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinProbePOp::HashJoinProbePOp(const std::string &name, HashJoinPredicate pred, std::set<std::string> neededColumnNames, long queryId) :
	PhysicalOp(name, "HashJoinProbe", queryId),
	kernel_(HashJoinProbeKernel2::make(std::move(pred), std::move(neededColumnNames))){
}

void HashJoinProbePOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "TupleSetIndexMessage") {
	auto hashTableMessage = dynamic_cast<const TupleSetIndexMessage &>(msg.message());
	this->onHashTable(hashTableMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
  }
}

void HashJoinProbePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinProbePOp::onTuple(const TupleMessage &msg) {
  // Incremental join immediately
  auto tupleSet = TupleSet2::create(msg.tuples());
  auto result = kernel_.joinProbeTupleSet(tupleSet);
  if(!result)
    throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

  // Send
  send(false);
}

void HashJoinProbePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    send(true);
  	ctx()->notifyComplete();
  }
}

void HashJoinProbePOp::onHashTable(const TupleSetIndexMessage &msg) {
  // Incremental join immediately
  auto result = kernel_.joinBuildTupleSetIndex(msg.getTupleSetIndex());
  if(!result)
    throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

  // Send
  send(false);
}

void HashJoinProbePOp::send(bool force) {
  auto buffer = kernel_.getBuffer();
  if (buffer.has_value() && (force || buffer.value()->numRows() >= DefaultBufferSize)) {
    std::shared_ptr<Message>
            tupleMessage = std::make_shared<TupleMessage>(buffer.value()->toTupleSetV1(), name());
    ctx()->tell(tupleMessage);
    kernel_.clear();
  }
}
