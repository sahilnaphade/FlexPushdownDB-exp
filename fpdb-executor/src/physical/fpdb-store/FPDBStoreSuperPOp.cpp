//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/executor/message/DebugMetricsMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/store/server/flight/SelectObjectContentTicket.hpp>
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

const std::shared_ptr<PhysicalPlan> &FPDBStoreSuperPOp::getSubPlan() const {
  return subPlan_;
}

void FPDBStoreSuperPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // try to process
  if (readyToProcess()) {
    processAtStore();
  }
}

void FPDBStoreSuperPOp::onBloomFilter(const BloomFilterMessage &msg) {
  std::optional<std::shared_ptr<bloomfilter::BloomFilterUsePOp>> bloomFilterUsePOp;

  for (const auto &opIt: subPlan_->getPhysicalOps()) {
    auto op = opIt.second;
    if (op->getType() == POpType::BLOOM_FILTER_USE) {
      bloomFilterUsePOp = std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op);
      break;
    }
  }

  if (!bloomFilterUsePOp.has_value()) {
    // no BloomFilterUsePOp found
    ctx()->notifyError("No BloomFilterUsePOp found");
  }

  // try to process
  (*bloomFilterUsePOp)->setBloomFilter(msg.getBloomFilter());
  if (readyToProcess()) {
    processAtStore();
  }
}

bool FPDBStoreSuperPOp::readyToProcess() {
  for (const auto &opIt: subPlan_->getPhysicalOps()) {
    auto op = opIt.second;
    if (op->getType() == POpType::BLOOM_FILTER_USE) {
      auto typedOp = std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op);
      if (!typedOp->receivedBloomFilter()) {
        return false;
      }
    }
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
  auto ticketObj = SelectObjectContentTicket::make(*expPlanString);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
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

  // send output tupleSet
  std::shared_ptr<TupleSet> tupleSet;
  if (table->num_rows() > 0) {
    tupleSet = TupleSet::make(table);
  } else {
    tupleSet = TupleSet::make(table->schema());
  }
  std::shared_ptr<Message> tupleMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
  ctx()->tell(tupleMessage);

  // metrics
#if SHOW_DEBUG_METRICS == true
  std::shared_ptr<Message> execMetricsMsg =
          std::make_shared<DebugMetricsMessage>(tupleSet->size(), this->name());
  ctx()->notifyRoot(execMetricsMsg);
#endif

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
