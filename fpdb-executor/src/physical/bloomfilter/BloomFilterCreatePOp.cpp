//
// Created by Yifei Yang on 3/16/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreatePOp::BloomFilterCreatePOp(const std::string &name,
                                           const std::vector<std::string> &projectColumnNames,
                                           int nodeId,
                                           const std::vector<std::string> &bloomFilterColumnNames):
  PhysicalOp(name, BLOOM_FILTER_CREATE, projectColumnNames, nodeId),
  kernel_(BloomFilterCreateKernel::make(bloomFilterColumnNames)) {}

std::string BloomFilterCreatePOp::getTypeString() const {
  return "BloomFilterCreatePOp";
}

void BloomFilterCreatePOp::onReceive(const Envelope &envelope) {
  const auto &msg = envelope.message();

  if (msg.type() == MessageType::START) {
    this->onStart();
  } else if (msg.type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg);
    this->onTupleSet(tupleSetMessage);
  } else if (msg.type() == MessageType::BLOOM_FILTER) {
    auto bloomFilterMessage = dynamic_cast<const BloomFilterMessage &>(msg);
    this->onBloomFilter(bloomFilterMessage);
  } else if (msg.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg);
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.getTypeString());
  }
}

void BloomFilterCreatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterCreatePOp::onTupleSet(const TupleSetMessage &msg) {
  auto result = kernel_.addTupleSet(msg.tuples());
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }
}

void BloomFilterCreatePOp::onBloomFilter(const BloomFilterMessage &msg) {
  auto result = kernel_.setBloomFilter(msg.getBloomFilter());
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }
}

void BloomFilterCreatePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    auto optBloomFilter = kernel_.getBloomFilter();
    if (!optBloomFilter.has_value()) {
      ctx()->notifyError("Bloom filter not create on complete");
    }

    std::shared_ptr<Message> bloomFilterMessage = std::make_shared<BloomFilterMessage>(*optBloomFilter, name());
    ctx()->tell(bloomFilterMessage);
    ctx()->notifyComplete();
  }
}

void BloomFilterCreatePOp::clear() {
  kernel_.clear();
}

}
