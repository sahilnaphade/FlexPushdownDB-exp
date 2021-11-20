//
// Created by matt on 17/6/20.
//

#include <normal/executor/physical/shuffle/ShufflePOp.h>
#include <normal/executor/physical/shuffle/ShuffleKernel2.h>
#include <normal/executor/physical/Globals.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ColumnBuilder.h>
#include <utility>

using namespace normal::executor::physical::shuffle;
using namespace normal::tuple;

ShufflePOp::ShufflePOp(std::string name,
                       std::string columnName,
                       std::vector<std::string> projectColumnNames,
                       long queryId) :
	PhysicalOp(std::move(name), "Shuffle", std::move(projectColumnNames), queryId),
	columnName_(std::move(columnName)) {}

void ShufflePOp::onReceive(const Envelope &msg) {
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
	throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}" + msg.message().type(), name()));
  }
}

void ShufflePOp::produce(const std::shared_ptr<PhysicalOp> &operator_) {
  PhysicalOp::produce(operator_);
  consumers_.emplace_back(operator_->name());
}

void ShufflePOp::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  columnName: {}, numConsumers: {}", name(), columnName_, consumers_.size());
  buffers_.resize(consumers_.size(), std::nullopt);
}

void ShufflePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    int partitionIndex = 0;
    for (const auto &buffer: buffers_) {
      auto sendResult = send(partitionIndex, true);
      if (!sendResult)
        throw std::runtime_error(sendResult.error());
      ++partitionIndex;
    }

    ctx()->notifyComplete();
  }
}

tl::expected<void, std::string> ShufflePOp::buffer(const std::shared_ptr<TupleSet> &tupleSet, int partitionIndex) {
  // Add the tuple set to the buffer
  if (!buffers_[partitionIndex].has_value()) {
	buffers_[partitionIndex] = tupleSet;
  } else {
    const auto &bufferedTupleSet = buffers_[partitionIndex].value();
	const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
	if (!concatenateResult)
	  return tl::make_unexpected(concatenateResult.error());
	buffers_[partitionIndex] = concatenateResult.value();

	std::shared_ptr<arrow::Table> combinedTable;
	auto expectedTable = buffers_[partitionIndex].value()->table()
		->CombineChunks(arrow::default_memory_pool());
	if (expectedTable.ok())
	  buffers_[partitionIndex] = TupleSet::make(*expectedTable);
	else
	  return tl::make_unexpected(expectedTable.status().message());
  }

  return {};
}

tl::expected<void, std::string> ShufflePOp::send(int partitionIndex, bool force) {
  // If the tupleset is big enough, send it, then clear the buffer
  if (buffers_[partitionIndex].has_value() && (force || buffers_[partitionIndex].value()->numRows() >= DefaultBufferSize)) {
	std::shared_ptr<Message> tupleMessage =
	        std::make_shared<TupleMessage>(buffers_[partitionIndex].value(), name());
	ctx()->send(tupleMessage, consumers_[partitionIndex]);
	buffers_[partitionIndex] = std::nullopt;
  }

  return {};
}

void ShufflePOp::onTuple(const TupleMessage &message) {
  // Get the tuple set
  const auto &tupleSet = message.tuples();
  std::vector<std::shared_ptr<TupleSet>> shuffledTupleSets;

  // Check empty
  if(tupleSet->numRows() == 0){
    for (size_t s = 0; s < consumers_.size(); ++s) {
      shuffledTupleSets.emplace_back(tupleSet);
    }
  }

  else {
    // Shuffle the tuple set
    auto expectedShuffledTupleSets = ShuffleKernel2::shuffle(columnName_, consumers_.size(), *tupleSet);
    if (!expectedShuffledTupleSets.has_value()) {
      throw std::runtime_error(fmt::format("{}, {}", expectedShuffledTupleSets.error(), name()));
    }
    shuffledTupleSets = expectedShuffledTupleSets.value();
  }

  // Send the shuffled tuple sets to consumers
  int partitionIndex = 0;
  for (const auto &shuffledTupleSet: shuffledTupleSets) {
    auto bufferAndSendResult = buffer(shuffledTupleSet, partitionIndex)
      .and_then([&]() { return send(partitionIndex, false); });
    if (!bufferAndSendResult)
      throw std::runtime_error(bufferAndSendResult.error());
    ++partitionIndex;
  }

}
