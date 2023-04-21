//
// Created by Yifei Yang on 3/16/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterUsePOp::BloomFilterUsePOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId,
                                     const std::vector<std::string> &bloomFilterColumnNames):
  PhysicalOp(name, BLOOM_FILTER_USE, projectColumnNames, nodeId),
  bloomFilterColumnNames_(bloomFilterColumnNames) {}

std::string BloomFilterUsePOp::getTypeString() const {
  return "BloomFilterUsePOp";
}

void BloomFilterUsePOp::onReceive(const Envelope &envelope) {
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

const std::vector<std::string> &BloomFilterUsePOp::getBloomFilterColumnNames() const {
  return bloomFilterColumnNames_;
}

const std::optional<std::shared_ptr<BloomFilterBase>> &BloomFilterUsePOp::getBloomFilter() const {
  return bloomFilter_;
}

void BloomFilterUsePOp::setCollPredTransMetrics(uint prePOpId,
                                                metrics::PredTransMetrics::PTMetricsUnitType ptMetricsType) {
  collPredTransMetrics_ = true;
  prePOpId_ = prePOpId;
  ptMetricsType_ = ptMetricsType;
}

void BloomFilterUsePOp::setBloomFilter(const std::shared_ptr<BloomFilterBase> &bloomFilter) {
  bloomFilter_ = bloomFilter;
}

bool BloomFilterUsePOp::receivedBloomFilter() const {
  return bloomFilter_.has_value();
}

void BloomFilterUsePOp::clearProducersExceptBloomFilterCreate() {
  for (auto it = producers_.begin(); it != producers_.end(); ) {
    if (it->substr(0, 17) != "BloomFilterCreate") {
      it = producers_.erase(it);
    }
    else {
      ++it;
    }
  }
}

void BloomFilterUsePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterUsePOp::onTupleSet(const TupleSetMessage &msg) {
  // Buffer tupleSet
  auto tupleSet = msg.tuples();
  if (!receivedTupleSet_.has_value()) {
    receivedTupleSet_ = tupleSet;
  }
  else {
    auto result = (*receivedTupleSet_)->append(tupleSet);
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }

  // Filter and send
  if (bloomFilter_.has_value()) {
    auto result = filterAndSend();
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }
}

void BloomFilterUsePOp::onBloomFilter(const BloomFilterMessage &msg) {
  bloomFilter_ = msg.getBloomFilter();

  // Filter and send
  if (receivedTupleSet_.has_value()) {
    auto result = filterAndSend();
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }
}

void BloomFilterUsePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    ctx()->notifyComplete();
  }
}

tl::expected<void, std::string> BloomFilterUsePOp::filterAndSend() {
  // Check
  if (!receivedTupleSet_.has_value()) {
    return tl::make_unexpected("No tupleSet received");
  }
  if (!bloomFilter_.has_value()) {
    return tl::make_unexpected("No bloom filter received");
  }

  // Filter
  auto expFilteredTupleSet = BloomFilterUseKernel::filter(*receivedTupleSet_,
                                                          *bloomFilter_,
                                                          bloomFilterColumnNames_);
  if (!expFilteredTupleSet.has_value()) {
    return tl::make_unexpected(expFilteredTupleSet.error());
  }
  auto filteredTupleSet = *expFilteredTupleSet;

  // Send
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(filteredTupleSet, name());
  ctx()->tell(tupleSetMessage);

#if SHOW_DEBUG_METRICS == true
  // predicate transfer metrics
  if (collPredTransMetrics_ && filteredTupleSet->numColumns() > 0) {
    std::shared_ptr<Message> ptMetricsMessage = std::make_shared<PredTransMetricsMessage>(
            metrics::PredTransMetrics::PTMetricsUnit(prePOpId_,
                                                     ptMetricsType_,
                                                     filteredTupleSet->schema(),
                                                     filteredTupleSet->numRows()),
            name_);
    ctx()->notifyRoot(ptMetricsMessage);
  }
#endif

  // Clear buffer
  receivedTupleSet_.reset();

  return {};
}

void BloomFilterUsePOp::clear() {
  receivedTupleSet_.reset();
  bloomFilter_.reset();
}

}
