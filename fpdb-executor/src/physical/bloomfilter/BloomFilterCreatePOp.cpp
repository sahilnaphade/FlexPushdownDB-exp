//
// Created by Yifei Yang on 3/16/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreatePOp::BloomFilterCreatePOp(const std::string &name,
                                           const std::vector<std::string> &projectColumnNames,
                                           int nodeId,
                                           const std::vector<std::string> &bloomFilterColumnNames,
                                           double desiredFalsePositiveRate):
  PhysicalOp(name, BLOOM_FILTER_CREATE, projectColumnNames, nodeId),
  kernel_(BloomFilterCreateKernel::make(bloomFilterColumnNames, desiredFalsePositiveRate)) {}

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
  } else if (msg.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg);
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.getTypeString());
  }
}

void BloomFilterCreatePOp::setBloomFilterUsePOp(const std::shared_ptr<PhysicalOp> &bloomFilterUsePOp) {
  bloomFilterUsePOp_ = bloomFilterUsePOp->name();
  PhysicalOp::produce(bloomFilterUsePOp);
}

void BloomFilterCreatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterCreatePOp::onTupleSet(const TupleSetMessage &msg) {
  // add tupleSet to kernel
  auto tupleSet = msg.tuples();
  auto result = kernel_.bufferTupleSet(tupleSet);
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }

  // pass tupleSet to consumers except bloomFilterUsePOp_
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
  auto consumers = consumers_;
  if (bloomFilterUsePOp_.has_value()) {
    consumers.erase(*bloomFilterUsePOp_);
  }
  ctx()->tell(tupleSetMessage, consumers);
}

void BloomFilterCreatePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // build and get bloom filter
    auto res = kernel_.buildBloomFilter();
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
    }
    auto bloomFilter = kernel_.getBloomFilter();
    if (!bloomFilter.has_value()) {
      ctx()->notifyError("Bloom filter not created on complete");
    }

    // send to bloomFilterUsePOp_
    std::shared_ptr<Message> bloomFilterMessage = std::make_shared<BloomFilterMessage>(*bloomFilter, name_);
    if (!bloomFilterUsePOp_.has_value()) {
      ctx()->notifyError("BloomFilterUsePOp not set when creating bloom filter");
    }
    ctx()->send(bloomFilterMessage, *bloomFilterUsePOp_);
    ctx()->notifyComplete();
  }
}

void BloomFilterCreatePOp::clear() {
  kernel_.clear();
}

}
