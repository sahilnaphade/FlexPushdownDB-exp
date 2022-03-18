//
// Created by Yifei Yang on 3/18/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePreparePOp.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreatePreparePOp::BloomFilterCreatePreparePOp(const std::string &name,
                                                         const std::vector<std::string> &projectColumnNames,
                                                         int nodeId,
                                                         double desiredFalsePositiveRate):
  PhysicalOp(name, BLOOM_FILTER_CREATE_PREPARE, projectColumnNames, nodeId),
  desiredFalsePositiveRate_(desiredFalsePositiveRate),
  capacity_(0) {}

std::string BloomFilterCreatePreparePOp::getTypeString() const {
  return "BloomFilterCreatePreparePOp";
}

void BloomFilterCreatePreparePOp::onReceive(const Envelope &envelope) {
  const auto &msg = envelope.message();

  if (msg.type() == MessageType::START) {
    this->onStart();
  } else if (msg.type() == MessageType::TUPLESET_SIZE) {
    auto tupleSetSizeMessage = dynamic_cast<const TupleSetSizeMessage &>(msg);
    this->onTupleSetSize(tupleSetSizeMessage);
  } else if (msg.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg);
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.getTypeString());
  }
}

void BloomFilterCreatePreparePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterCreatePreparePOp::onTupleSetSize(const TupleSetSizeMessage &msg) {
  auto numRows = msg.getNumRows();
  capacity_ += numRows;
}

void BloomFilterCreatePreparePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // create an empty bloom filter for each parallel BloomFilterCreatePOp to make a copy from it
    auto bloomFilter = BloomFilter::make(capacity_, desiredFalsePositiveRate_);
    bloomFilter->init();

    // send and complete
    std::shared_ptr<Message> bloomFilterMessage = std::make_shared<BloomFilterMessage>(bloomFilter, name());
    ctx()->tell(bloomFilterMessage);
    ctx()->notifyComplete();
  }
}

void BloomFilterCreatePreparePOp::clear() {
  // noop
}

}
