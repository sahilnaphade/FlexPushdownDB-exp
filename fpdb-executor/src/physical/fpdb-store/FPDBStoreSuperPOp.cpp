//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/DebugMetricsMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/executor/caf/CAFAdaptPushdownUtil.h>
#include <fpdb/executor/FPDBStoreExecution.h>
#include <fpdb/store/server/flight/SelectObjectContentTicket.hpp>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <fpdb/store/server/flight/ClearBitmapCmd.hpp>
#include <fpdb/store/server/flight/Util.hpp>
#include <arrow/flight/api.h>
#include <unordered_map>

using namespace fpdb::store::server::flight;

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreSuperPOp::FPDBStoreSuperPOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId,
                                     const std::shared_ptr<PhysicalPlan> &subPlan,
                                     const std::string &host,
                                     int fileServicePort,
                                     int flightPort):
  PhysicalOp(name, POpType::FPDB_STORE_SUPER, projectColumnNames, nodeId),
  subPlan_(subPlan),
  host_(host),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

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

int FPDBStoreSuperPOp::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreSuperPOp::getFlightPort() const {
  return flightPort_;
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
  // set both forwardConsumers and endConsumers for root
  std::vector<std::string> endConsumers;
  std::unordered_map<std::string, std::string> forwardConsumerMap;
  int i = 0;
  for (const auto &producer: producers) {
    const auto &consumer = consumers[i++]->name();
    forwardConsumerMap[producer] = consumer;
    endConsumers.emplace_back(consumer);
  }
  collatePOp->setForward(true);
  collatePOp->setForwardConsumers(forwardConsumerMap);
  collatePOp->setEndConsumers(endConsumers);
}

void FPDBStoreSuperPOp::setGetAdaptPushdownMetrics(bool getAdaptPushdownMetrics) {
  getAdaptPushdownMetrics_ = getAdaptPushdownMetrics;
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
  auto status = arrow::flight::Location::ForGrpcTcp(host_, flightPort_, &clientLocation);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }

  arrow::flight::FlightClientOptions clientOptions = arrow::flight::FlightClientOptions::Defaults();
  std::unique_ptr<arrow::flight::FlightClient> client;
  status = arrow::flight::FlightClient::Connect(clientLocation, clientOptions, &client);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }

  // send request to store
  auto expPlanString = serialize(false);
  if (!expPlanString.has_value()) {
    ctx()->notifyError(expPlanString.error());
    return;
  }
  auto ticketObj = SelectObjectContentTicket::make(queryId_, name_, *expPlanString);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }

  // if pushdown result should be received by consumers in pipeline, let them start waiting now
  if (receiveByOthers_) {
    std::shared_ptr<Message> tupleSetWaitRemoteMessage =
            std::make_shared<TupleSetWaitRemoteMessage>(host_, flightPort_, name_);
    ctx()->tell(tupleSetWaitRemoteMessage);
  }

  auto startTime = std::chrono::steady_clock::now();
  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
  status = client->DoGet(*expTicket, &reader);
  if (!status.ok()) {
    auto flightStatusDetail = std::static_pointer_cast<arrow::flight::FlightStatusDetail>(status.detail());
    // the request is rejected by storage due to resource limitation
    if (ENABLE_ADAPTIVE_PUSHDOWN && flightStatusDetail->code() == ReqRejectStatusCode) {
      // currently unsupported if "receiveByOthers_" = true
      if (receiveByOthers_) {
        ctx()->notifyError("Adaptive pushdown with pipelining pushdown results is currently unsupported");
        return;
      }
      // fall back to pullup (adaptive pushdown)
      processAsPullup();
      // complete and return
      ctx()->notifyComplete();
      return;
    }

    // error
    else {
      ctx()->notifyError(status.message());
      return;
    }
  }

  std::shared_ptr<::arrow::Table> table;
  status = reader->ReadAll(&table);
  if (!status.ok()) {
    ctx()->notifyError(status.message());
    return;
  }
  auto stopTime = std::chrono::steady_clock::now();

  // for metrics of adaptive pushdown
  if (getAdaptPushdownMetrics_) {
    auto expAdaptPushdownMetricsKey = AdaptPushdownMetricsMessage::generateAdaptPushdownMetricsKey(queryId_, name_);
    if (!expAdaptPushdownMetricsKey.has_value()) {
      ctx()->notifyError(expAdaptPushdownMetricsKey.error());
      return;
    }
    int64_t execTime = std::chrono::duration_cast<chrono::nanoseconds>(stopTime - startTime).count();
    std::shared_ptr<Message> adaptPushdownMetricsMessage = std::make_shared<AdaptPushdownMetricsMessage>(
            *expAdaptPushdownMetricsKey, execTime, name_);
    ctx()->notifyRoot(adaptPushdownMetricsMessage);
  }

  // if pushdown result hasn't been waiting by consumers
  if (!receiveByOthers_) {
    // if not having shuffle op, do regularly
    if (!shufflePOpName_.has_value()) {
      // check table
      if (table == nullptr) {
        ctx()->notifyError("Received null table from FPDB-Store");
        return;
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
              std::make_shared<DebugMetricsMessage>(metrics::DebugMetrics(tupleSet->size(), 0, 0), this->name());
      ctx()->notifyRoot(execMetricsMsg);
#endif
    }

      // if having shuffle op
    else {
      std::shared_ptr<Message> tupleSetReadyRemoteMessage =
              std::make_shared<TupleSetReadyRemoteMessage>(host_, flightPort_, true, name_);
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
  auto client = makeDoPutFlightClient(host_, flightPort_);

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
        return;
      }
      auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
      std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
      std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
      auto status = client->DoPut(descriptor, nullptr, &writer, &metadataReader);
      if (!status.ok()) {
        ctx()->notifyError(status.message());
        return;
      }
      status = writer->Close();
      if (!status.ok()) {
        ctx()->notifyError(status.message());
        return;
      }
    }
  }

  // complete
  ctx()->notifyComplete();
}

void FPDBStoreSuperPOp::processAsPullup() {
  // update "subPlan_", by changing FPDBFileScanPOp to RemoteFileScanPOp
  auto res = subPlan_->fallBackToPullup(host_, fileServicePort_);
  if (!res.has_value()) {
    ctx()->notifyError(res.error());
    return;
  }

  // execute subPlan
  auto execution = std::make_shared<FPDBStoreExecution>(
          queryId_, caf::CAFAdaptPushdownUtil::daemonAdaptPushdownActorSystem_, subPlan_,
          [&] (const std::string &consumer, const std::shared_ptr<arrow::Table> &table) {
            std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(TupleSet::make(table), name_);
            ctx()->send(tupleSetMessage, consumer);
          },
          [&] (const std::string &, const std::vector<int64_t> &) {
            // noop
          });
  auto tupleSet = execution->execute();
  if (tupleSet == nullptr || tupleSet->table() == nullptr) {
    ctx()->notifyError("Received null table from fallback to pullup execution");
    return;
  }
  if (tupleSet->numRows() != 0 || tupleSet->numColumns() != 0) {
    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
    ctx()->tell(tupleSetMessage);
  }

  // metrics
#if SHOW_DEBUG_METRICS == true
  std::shared_ptr<Message> execMetricsMsg = std::make_shared<DebugMetricsMessage>(execution->getDebugMetrics(), name_);
  ctx()->notifyRoot(execMetricsMsg);
#endif
}

tl::expected<std::string, std::string> FPDBStoreSuperPOp::serialize(bool pretty) {
  return PhysicalPlanSerializer::serialize(subPlan_, pretty);
}

void FPDBStoreSuperPOp::clear() {
  subPlan_.reset();
}

}
