//
// Created by Yifei Yang on 3/16/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/store/server/flight/PutBitmapCmd.hpp>

using namespace fpdb::store::server::flight;

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

void BloomFilterCreatePOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  passTupleSetConsumers_.emplace(op->name());
  PhysicalOp::produce(op);
}

void BloomFilterCreatePOp::addBloomFilterUsePOp(const std::shared_ptr<PhysicalOp> &bloomFilterUsePOp) {
  bloomFilterUsePOps_.emplace(bloomFilterUsePOp->name());
  PhysicalOp::produce(bloomFilterUsePOp);
}

void BloomFilterCreatePOp::setBloomFilterInfo(const fpdb_store::FPDBStoreBloomFilterCreateInfo &bloomFilterInfo) {
  bloomFilterInfo_ = bloomFilterInfo;
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

  // pass tupleSet to consumers except bloomFilterUsePOps_
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
  ctx()->tell(tupleSetMessage, passTupleSetConsumers_);
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

    // send bloom filter to bloomFilterUsePOps_
    std::shared_ptr<Message> bloomFilterMessage = std::make_shared<BloomFilterMessage>(*bloomFilter, name_);
    ctx()->tell(bloomFilterMessage, bloomFilterUsePOps_);

    // send bloom filter to fpdb-store if needed
    if (bloomFilterInfo_.has_value()) {
      putBloomFilterToStore(*bloomFilter);
    }

    ctx()->notifyComplete();
  }
}

void BloomFilterCreatePOp::putBloomFilterToStore(const std::shared_ptr<BloomFilter> &bloomFilter) {
  // make flight client and connect
  makeDoPutFlightClient(bloomFilterInfo_->host_, bloomFilterInfo_->port_);

  // send request to store
  auto bitmap = bloomFilter->getBitArray();
  auto expRecordBatch = ArrowSerializer::bitmap_to_recordBatch(bitmap);
  if (!expRecordBatch.has_value()) {
    ctx()->notifyError(expRecordBatch.error());
  }
  auto recordBatch = *expRecordBatch;

  auto cmdObj = PutBitmapCmd::make(BitmapType::BLOOM_FILTER_COMPUTE, queryId_, name_, true,
                                   bloomFilterInfo_->numCopies_, bloomFilter->toJson());
  auto expCmd = cmdObj->serialize(false);
  if (!expCmd.has_value()) {
    ctx()->notifyError(expCmd.error());
  }
  auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
  auto status = (*DoPutFlightClient)->DoPut(descriptor, recordBatch->schema(), &writer, &metadataReader);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }

  status = writer->WriteRecordBatch(*recordBatch);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }
  status = writer->DoneWriting();
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }
  status = writer->Close();
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }
}

void BloomFilterCreatePOp::clear() {
  kernel_.clear();
}

}
