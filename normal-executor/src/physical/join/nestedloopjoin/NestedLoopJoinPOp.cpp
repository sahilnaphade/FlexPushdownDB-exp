//
// Created by Yifei Yang on 12/12/21.
//

#include <normal/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <normal/executor/physical/Globals.h>

namespace normal::executor::physical::join {

NestedLoopJoinPOp::NestedLoopJoinPOp(const string &name,
                                     const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                     JoinType joinType,
                                     const vector<string> &projectColumnNames) :
  PhysicalOp(name, "NestedLoopJoinPOp", projectColumnNames) {

  set<string> neededColumnNames(getProjectColumnNames().begin(), getProjectColumnNames().end());
  switch (joinType) {
    case INNER: {
      kernel_ = NestedLoopJoinKernel::make(predicate, neededColumnNames, false, false);
      break;
    }
    case LEFT: {
      kernel_ = NestedLoopJoinKernel::make(predicate, neededColumnNames, true, false);
      break;
    }
    case RIGHT: {
      kernel_ = NestedLoopJoinKernel::make(predicate, neededColumnNames, false, true);
      break;
    }
    case FULL: {
      kernel_ = NestedLoopJoinKernel::make(predicate, neededColumnNames, true, true);
      break;
    }
    default:
      throw runtime_error(fmt::format("Unsupported nested loop join type, {}", joinType));
  }
}

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
    send(true);
    ctx()->notifyComplete();
  }
}

void NestedLoopJoinPOp::onTuple(const TupleMessage &message) {
  const auto &tupleSet = message.tuples();
  const auto &sender = message.sender();

  // incremental join immediately
  tl::expected<void, string> result;
  if (leftProducerNames_.find(sender) != leftProducerNames_.end()) {
    result = kernel_->joinIncomingLeft(tupleSet);
  } else if (rightProducerName_.find(sender) != rightProducerName_.end()) {
    result = kernel_->joinIncomingRight(tupleSet);
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

}
