//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/DebugMetricsMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/store/server/flight/SelectObjectContentTicket.hpp>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <fpdb/store/server/flight/ClearBitmapCmd.hpp>
#include <arrow/flight/api.h>
#include <unordered_map>

using namespace fpdb::store::server::flight;

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreSuperPOp::FPDBStoreSuperPOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId,
                                     const std::shared_ptr<PhysicalPlan> &subPlan,
                                     const std::string &host,
                                     int port):
  PhysicalOp(name, POpType::FPDB_STORE_SUPER, projectColumnNames, nodeId),
  subPlan_(subPlan),
  host_(host),
  port_(port) {}

void FPDBStoreSuperPOp::onReceive(const Envelope &envelope) {
  const auto &message = envelope.message();

  if (message.type() == MessageType::START) {
    this->onStart();
  } else if (message.type() == MessageType::SCAN) {
    auto scanMessage = dynamic_cast<const ScanMessage &>(message);
    this->onCacheLoadResponse(scanMessage);
  } else if (message.type() == MessageType::BLOOM_FILTER) {
    auto bloomFilterMessage = dynamic_cast<const BloomFilterMessage &>(message);
    this->onBloomFilter(bloomFilterMessage);
  } else if (message.type() == MessageType::COMPLETE) {
    // noop
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

std::string FPDBStoreSuperPOp::getTypeString() const {
  return "FPDBStoreSuperPOp";
}

void FPDBStoreSuperPOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);

  // need to add op to consumerVec_ of shuffle op explicitly
  if (shufflePOpName_.has_value()) {
    auto expShufflePOp = subPlan_->getPhysicalOp(*shufflePOpName_);
    if (!expShufflePOp.has_value()) {
      throw std::runtime_error(expShufflePOp.error());
    }
    std::static_pointer_cast<shuffle::ShufflePOp>(*expShufflePOp)->addToConsumerVec(op);
  }
}

const std::shared_ptr<PhysicalPlan> &FPDBStoreSuperPOp::getSubPlan() const {
  return subPlan_;
}

const std::string &FPDBStoreSuperPOp::getHost() const {
  return host_;
}

int FPDBStoreSuperPOp::getPort() const {
  return port_;
}

void FPDBStoreSuperPOp::setWaitForScanMessage(bool waitForScanMessage) {
  waitForScanMessage_ = waitForScanMessage;
}

void FPDBStoreSuperPOp::setReceiveByOthers(bool receiveByOthers) {
  receiveByOthers_ = receiveByOthers;
}

void FPDBStoreSuperPOp::setShufflePOp(const std::shared_ptr<PhysicalOp> &op) {
  shufflePOpName_ = op->name();
}

void FPDBStoreSuperPOp::addFPDBStoreBloomFilterProducer(
        const std::shared_ptr<PhysicalOp> &fpdbStoreBloomFilterProducer) {
  ++numBloomFiltersExpected_;
  PhysicalOp::consume(fpdbStoreBloomFilterProducer);
}

void FPDBStoreSuperPOp::setForwardConsumers(const std::vector<std::shared_ptr<PhysicalOp>> &consumers) {
  auto expRootPOp = subPlan_->getRootPOp();
  if (!expRootPOp.has_value()) {
    throw std::runtime_error(expRootPOp.error());
  }
  auto collatePOp = std::static_pointer_cast<collate::CollatePOp>(*expRootPOp);
  const auto &producers = collatePOp->producers();
  if (producers.size() != consumers.size()) {
    throw std::runtime_error(fmt::format("num producers ({}) and num forward consumers ({}) mismatch on \"set\"",
                             producers.size(), consumers.size()));
  }
  std::unordered_map<std::string, std::string> forwardConsumerMap;
  int i = 0;
  for (const auto &producer: producers) {
    forwardConsumerMap[producer] = consumers[i++]->name();
  }
  collatePOp->setForward(true);
  collatePOp->setForwardConsumers(forwardConsumerMap);
}

void FPDBStoreSuperPOp::resetForwardConsumers() {
  auto expRootPOp = subPlan_->getRootPOp();
  if (!expRootPOp.has_value()) {
    throw std::runtime_error(expRootPOp.error());
  }
  auto collatePOp = std::static_pointer_cast<collate::CollatePOp>(*expRootPOp);
  const auto &producers = collatePOp->producers();
  const auto &forwardConsumerMap = collatePOp->getForwardConsumers();
  if (producers.size() != forwardConsumerMap.size()) {
    throw std::runtime_error(fmt::format("num producers ({}) and num forward consumers ({}) mismatch on \"reset\"",
                                         producers.size(), forwardConsumerMap.size()));
  }
  std::unordered_map<std::string, std::string> newForwardConsumerMap;
  auto forwardConsumerIt = forwardConsumerMap.begin();
  for (const auto &producer: producers) {
    newForwardConsumerMap[producer] = forwardConsumerIt->second;
    ++forwardConsumerIt;
  }
  collatePOp->setForwardConsumers(newForwardConsumerMap);
}

void FPDBStoreSuperPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // try to process
  if (readyToProcess()) {
    processAtStore();
  }
}

void FPDBStoreSuperPOp::onCacheLoadResponse(const ScanMessage &msg) {
  // check if nothing to process (i.e. no columns in scan message)
  auto scanColumnNames = msg.getColumnNames();
  if (scanColumnNames.empty()) {
    processEmpty();
    return;
  }

  // set project column names
  for (const auto &opIt: subPlan_->getPhysicalOps()) {
    auto op = opIt.second;
    if (op->getType() == POpType::FPDB_STORE_FILE_SCAN) {
      op->setProjectColumnNames(scanColumnNames);
      break;
    }
  }

  // set flag
  waitForScanMessage_ = false;

  // try to process
  if (readyToProcess()) {
    processAtStore();
  }
}

void FPDBStoreSuperPOp::onBloomFilter(const BloomFilterMessage &) {
  // this is just to notify that one bloom filter has been sent to store, no real bloom filter in the msg
  ++numBloomFiltersReceived_;

  // try to process
  if (readyToProcess()) {
    processAtStore();
  }
}

bool FPDBStoreSuperPOp::readyToProcess() {
  // check if waiting for scan message, specifically in hybrid mode
  if (waitForScanMessage_) {
    return false;
  }

  // check if all bloom filters needed have been sent to store
  if (numBloomFiltersReceived_ < numBloomFiltersExpected_) {
    return false;
  }

  return true;
}

void FPDBStoreSuperPOp::processAtStore() {
  // make flight client and connect
  arrow::flight::Location clientLocation;
  auto status = arrow::flight::Location::ForGrpcTcp(host_, port_, &clientLocation);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }

  arrow::flight::FlightClientOptions clientOptions = arrow::flight::FlightClientOptions::Defaults();
  std::unique_ptr<arrow::flight::FlightClient> client;
  status = arrow::flight::FlightClient::Connect(clientLocation, clientOptions, &client);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }

  // send request to store
  auto expPlanString = serialize(false);
  if (!expPlanString.has_value()) {
    ctx()->notifyError(expPlanString.error());
  }
  auto ticketObj = SelectObjectContentTicket::make(queryId_, name_, *expPlanString);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
  }

  // if pushdown result should be received by consumers, let them start waiting now
  if (receiveByOthers_) {
    std::shared_ptr<Message> tupleSetWaitRemoteMessage =
            std::make_shared<TupleSetWaitRemoteMessage>(host_, port_, name_);
    ctx()->tell(tupleSetWaitRemoteMessage);
  }

  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
  status = client->DoGet(*expTicket, &reader);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }

  std::shared_ptr<::arrow::Table> table;
  status = reader->ReadAll(&table);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
  }

  // if pushdown result hasn't been waiting by consumers
  if (!receiveByOthers_) {
    // if not having shuffle op, do regularly
    if (!shufflePOpName_.has_value()) {
      // check table
      if (table == nullptr) {
        ctx()->notifyError("Received null table from FPDB-Store");
      }

      // send output tupleSet
      std::shared_ptr<TupleSet> tupleSet;
      if (table->num_rows() > 0) {
        tupleSet = TupleSet::make(table);
      } else {
        tupleSet = TupleSet::make(table->schema());
      }
      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
      ctx()->tell(tupleSetMessage);

      // metrics
#if SHOW_DEBUG_METRICS == true
      std::shared_ptr<Message> execMetricsMsg =
              std::make_shared<DebugMetricsMessage>(tupleSet->size(), this->name());
      ctx()->notifyRoot(execMetricsMsg);
#endif
    }

      // if having shuffle op
    else {
      std::shared_ptr<Message> tupleSetReadyRemoteMessage =
              std::make_shared<TupleSetReadyRemoteMessage>(host_, port_, name_);
      ctx()->tell(tupleSetReadyRemoteMessage);
    }
  }

  // complete
  ctx()->notifyComplete();
}

void FPDBStoreSuperPOp::processEmpty() {
  // send an empty tupleSet
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(TupleSet::makeWithEmptyTable(), name_);
  ctx()->tell(tupleSetMessage);

  // need to clear bitmaps cached at storage if bitmap pushdown is enabled for some ops
  // make flight client and connect
  auto client = makeDoPutFlightClient(host_, port_);

  for (const auto &opIt: subPlan_->getPhysicalOps()) {
    auto op = opIt.second;

    // clear bitmap cached at storage for each FilterPOp
    if (op->getType() == POpType::FILTER) {
      auto typedOp = std::static_pointer_cast<filter::FilterPOp>(op);
      if (!typedOp->isBitmapPushdownEnabled()) {
        continue;
      }

      // send request to store
      auto cmdObj = ClearBitmapCmd::make(BitmapType::FILTER_COMPUTE, queryId_, typedOp->getBitmapWrapper()->mirrorOp_);
      auto expCmd = cmdObj->serialize(false);
      if (!expCmd.has_value()) {
        ctx()->notifyError(expCmd.error());
      }
      auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
      std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
      std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
      auto status = client->DoPut(descriptor, nullptr, &writer, &metadataReader);
      if (!status.ok()) {
        ctx()->notifyError(status.message());
      }
      status = writer->Close();
      if (!status.ok()) {
        ctx()->notifyError(status.message());
      }
    }
  }

  // complete
  ctx()->notifyComplete();
}

tl::expected<std::string, std::string> FPDBStoreSuperPOp::serialize(bool pretty) {
  return PhysicalPlanSerializer::serialize(subPlan_, pretty);
}

void FPDBStoreSuperPOp::clear() {
  subPlan_.reset();
}

}
