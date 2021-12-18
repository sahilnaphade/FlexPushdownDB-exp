//
// Created by matt on 29/4/20.
//

#include <normal/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <normal/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/tuple/arrow/SchemaHelper.h>
#include <utility>

using namespace normal::executor::physical::join;

HashJoinProbePOp::HashJoinProbePOp(string name,
                                   HashJoinPredicate pred,
                                   JoinType joinType,
                                   vector<string> projectColumnNames) :
	PhysicalOp(move(name), "HashJoinProbePOp", move(projectColumnNames)) {

  set<string> neededColumnNames(getProjectColumnNames().begin(), getProjectColumnNames().end());
  switch (joinType) {
    case INNER: {
      kernel_ = HashJoinProbeKernel::make(move(pred), move(neededColumnNames));
      break;
    }
    case SEMI: {
      kernel_ = HashSemiJoinProbeKernel::make(move(pred), move(neededColumnNames));
      break;
    }
    default:
      throw runtime_error(fmt::format("Unsupported hash join type, {}", joinType));
  }
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
	throw runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
  }
}

void HashJoinProbePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinProbePOp::onTuple(const TupleMessage &msg) {
  // Incremental join immediately
  const auto& tupleSet = msg.tuples();
  auto result = kernel_->joinProbeTupleSet(tupleSet);
  if(!result)
    throw runtime_error(fmt::format("{}, {}", result.error(), name()));

  // Send
  send(false);
}

void HashJoinProbePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // Finalize
    auto result = kernel_->finalize();
    if (!result) {
      throw runtime_error(result.error());
    }

    // Send final tupleSet
    send(true);

    // Complete
  	ctx()->notifyComplete();
  }
}

void HashJoinProbePOp::onHashTable(const TupleSetIndexMessage &msg) {
  // Incremental join immediately
  auto result = kernel_->joinBuildTupleSetIndex(msg.getTupleSetIndex());
  if(!result)
    throw runtime_error(fmt::format("{}, {}", result.error(), name()));

  // Send
  send(false);
}

void HashJoinProbePOp::send(bool force) {
  auto buffer = kernel_->getBuffer();
  if (buffer.has_value()) {
    auto numRows = buffer.value()->numRows();
    if (numRows >= DefaultBufferSize || (force && numRows > 0)) {
      // Project using projectColumnNames
      auto expProjectTupleSet = TupleSet::make(buffer.value()->table())->projectExist(getProjectColumnNames());
      if (!expProjectTupleSet) {
        throw runtime_error(expProjectTupleSet.error());
      }

      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), name());
      ctx()->tell(tupleMessage);
      kernel_->clearBuffer();
    }
  }
}