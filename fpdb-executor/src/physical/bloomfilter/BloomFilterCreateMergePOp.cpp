//
// Created by Yifei Yang on 3/18/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateMergePOp.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreateMergePOp::BloomFilterCreateMergePOp(const std::string &name,
                                                     const std::vector<std::string> &projectColumnNames,
                                                     int nodeId):
  PhysicalOp(name, BLOOM_FILTER_CREATE_MERGE, projectColumnNames, nodeId),
  nBloomFilterReceived_(0) {}

std::string BloomFilterCreateMergePOp::getTypeString() const {
  return "BloomFilterCreateMergePOp";
}

void BloomFilterCreateMergePOp::onReceive(const Envelope &envelope) {
  const auto &msg = envelope.message();

  if (msg.type() == MessageType::START) {
    this->onStart();
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

void BloomFilterCreateMergePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterCreateMergePOp::onBloomFilter(const BloomFilterMessage &msg) {
  auto bloomFilter = msg.getBloomFilter();
  ++nBloomFilterReceived_;

  if (!mergedBloomFilter_.has_value()) {
    mergedBloomFilter_ = bloomFilter;
  } else {
    auto result = (*mergedBloomFilter_)->merge(bloomFilter);
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }
}

void BloomFilterCreateMergePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // check
    if (nBloomFilterReceived_ != (int) producers_.size()) {
      ctx()->notifyError(fmt::format("num bloomFilters received and num producers mismatch, {} vs {}",
                                     nBloomFilterReceived_, producers_.size()));
    }

    // send and complete
    std::shared_ptr<Message> bloomFilterMessage = std::make_shared<BloomFilterMessage>(*mergedBloomFilter_, name());
    ctx()->tell(bloomFilterMessage);
    ctx()->notifyComplete();
  }
}

void BloomFilterCreateMergePOp::clear() {
  mergedBloomFilter_.reset();
}
  
}
