//
// Created by Yifei Yang on 12/12/21.
//

#include <normal/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <normal/executor/physical/Globals.h>

namespace normal::executor::physical::join {

NestedLoopJoinPOp::NestedLoopJoinPOp(const string &name,
                                     const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                     JoinType joinType,
                                     const vector<string> &projectColumnNames) :
  PhysicalOp(name, "NestedLoopJoinPOp", projectColumnNames),
  kernel_(makeKernel(predicate, joinType)) {}

NestedLoopJoinKernel
NestedLoopJoinPOp::makeKernel(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                              JoinType joinType) {
  set<string> neededColumnNames(projectColumnNames_.begin(), projectColumnNames_.end());
  if (predicate.has_value()) {
    const auto &predInvolvedColumnNames = (*predicate)->involvedColumnNames();
    neededColumnNames.insert(predInvolvedColumnNames.begin(), predInvolvedColumnNames.end());
  }
  switch (joinType) {
    case INNER: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, false, false);
    }
    case LEFT: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, true, false);
    }
    case RIGHT: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, false, true);
    }
    case FULL: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, true, true);
    }
    default:
      throw runtime_error(fmt::format("Unsupported nested loop join type, {}", joinType));
  }
};

void NestedLoopJoinPOp::onReceive(const Envelope &msg) {
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

void NestedLoopJoinPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void NestedLoopJoinPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // Finalize
    auto result = kernel_.finalize();
    if (!result) {
      throw runtime_error(result.error());
    }

    // Send final tupleSet
    send(true);

    // Send empty if no result
    if (!sentResult) {
      sendEmpty();
    }

    // Complete
    ctx()->notifyComplete();
  }
}

void NestedLoopJoinPOp::onTuple(const TupleMessage &message) {
  const auto &tupleSet = message.tuples();
  const auto &sender = message.sender();

  // incremental join immediately
  tl::expected<void, string> result;
  if (leftProducerNames_.find(sender) != leftProducerNames_.end()) {
    result = kernel_.joinIncomingLeft(tupleSet);
  } else if (rightProducerName_.find(sender) != rightProducerName_.end()) {
    result = kernel_.joinIncomingRight(tupleSet);
  } else {
    throw runtime_error(fmt::format("Unknown sender '{}', neither left nor right producer", sender));
  }
  if (!result.has_value()) {
    throw runtime_error(result.error());
  }

  // send result if exceeding buffer size
  send(false);
}

void NestedLoopJoinPOp::addLeftProducer(const shared_ptr<PhysicalOp> &leftProducer) {
  leftProducerNames_.emplace(leftProducer->name());
  consume(leftProducer);
}

void NestedLoopJoinPOp::addRightProducer(const shared_ptr<PhysicalOp> &rightProducer) {
  rightProducerName_.emplace(rightProducer->name());
  consume(rightProducer);
}

void NestedLoopJoinPOp::send(bool force) {
  auto buffer = kernel_.getBuffer();
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
      sentResult = true;
      kernel_.clearBuffer();
    }
  }
}

void NestedLoopJoinPOp::sendEmpty() {
  auto outputSchema = kernel_.getOutputSchema();
  if (!outputSchema.has_value()) {
    throw runtime_error("OutputSchema not set yet");
  }
  auto expProjectTupleSet = TupleSet::make(outputSchema.value())->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet.has_value()) {
    throw runtime_error(expProjectTupleSet.error());
  }

  shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), name());
  ctx()->tell(tupleMessage);
}

}
