//
// Created by matt on 20/7/20.
//

#include <normal/executor/physical/merge/MergePOp.h>
#include <normal/executor/physical/merge/MergeKernel.h>

using namespace normal::executor::physical::merge;

MergePOp::MergePOp(const std::string &name,
                   const std::vector<std::string> &projectColumnNames,
                   long queryId) :
	PhysicalOp(name, "Merge", projectColumnNames, queryId) {
}

void MergePOp::onReceive(const Envelope &msg) {
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

void MergePOp::onStart() {

  SPDLOG_DEBUG("Starting operator  |  name: '{}', leftProducer: {}, rightProducer: {}",
			   name(),
			   leftProducer_.lock()->name(),
			   rightProducer_.lock()->name());
}

void MergePOp::merge() {
  // Check if we have merge-able tuple sets
  while (!leftTupleSets_.empty() && !rightTupleSets_.empty()) {

    // Take next left and right tuplesets from the queues
    auto leftTupleSet = leftTupleSets_.front();
    auto rightTupleSet = rightTupleSets_.front();

    // Merge tuplesets
    auto expectedMergedTupleSet = MergeKernel::merge(leftTupleSet, rightTupleSet);

    if (!expectedMergedTupleSet.has_value()) {
      throw std::runtime_error(fmt::format("{}.\n leftOp: {}\n rightOp: {}",
                                expectedMergedTupleSet.error(), leftProducer_.lock()->name(), rightProducer_.lock()->name()));
    } else {
      // Send merged tupleset
      auto mergedTupleSet = expectedMergedTupleSet.value();
      std::shared_ptr<Message>
              tupleMessage = std::make_shared<TupleMessage>(mergedTupleSet->toTupleSetV1(), name());
      ctx()->tell(tupleMessage);
    }

    // Pop the processed tuple sets from the queues
    leftTupleSets_.pop_front();
    rightTupleSets_.pop_front();
  }
}

void MergePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
	  ctx()->notifyComplete();
  }
}

void MergePOp::onTuple(const TupleMessage &message) {
  // Get the tuple set
  const auto &tupleSet = TupleSet2::create(message.tuples());

  // Add the tupleset to a slot in left or right producers tuple queue
  if (message.sender() == leftProducer_.lock()->name()) {
	leftTupleSets_.emplace_back(tupleSet);
  } else if (message.sender() == rightProducer_.lock()->name()) {
	rightTupleSets_.emplace_back(tupleSet);
  } else {
	throw std::runtime_error(fmt::format("Unrecognized producer {}, left: {}, right: {}",
	        message.sender(), leftProducer_.lock()->name(), rightProducer_.lock()->name()));
  }

  // Merge
  merge();
}

void MergePOp::setLeftProducer(const std::shared_ptr<PhysicalOp> &leftProducer) {
  leftProducer_ = leftProducer;
  consume(leftProducer);
}

void MergePOp::setRightProducer(const std::shared_ptr<PhysicalOp> &rightProducer) {
  rightProducer_ = rightProducer;
  consume(rightProducer);
}
