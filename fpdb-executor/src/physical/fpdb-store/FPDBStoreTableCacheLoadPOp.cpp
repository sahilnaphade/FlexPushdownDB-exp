//
// Created by Yifei Yang on 10/6/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreTableCacheLoadPOp.h>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <fpdb/store/server/flight/Util.hpp>
#include <arrow/flight/api.h>

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreTableCacheLoadPOp::FPDBStoreTableCacheLoadPOp(const std::string &name,
                                                       const std::vector<std::string> &projectColumnNames,
                                                       int nodeId):
  PhysicalOp(name, POpType::FPDB_STORE_TABLE_CACHE_LOAD, projectColumnNames, nodeId) {}

void FPDBStoreTableCacheLoadPOp::onReceive(const Envelope &envelope) {
  const auto &message = envelope.message();

  if (message.type() == MessageType::START) {
    this->onStart();
  } else if (message.type() == MessageType::TUPLESET_WAIT_REMOTE) {
    auto tupleSetWaitRemoteMessage = dynamic_cast<const TupleSetWaitRemoteMessage &>(message);
    this->onTupleSetWaitRemote(tupleSetWaitRemoteMessage);
  } else if (message.type() == MessageType::COMPLETE) {
    // noop
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

std::string FPDBStoreTableCacheLoadPOp::getTypeString() const {
  return "FPDBStoreTableCacheLoadPOp";
}

void FPDBStoreTableCacheLoadPOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  if (!producers_.empty()) {
    throw std::runtime_error("FPDBStoreTableCacheLoadPOp should only have one producer.");
  }
  PhysicalOp::consume(op);
}

void FPDBStoreTableCacheLoadPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void FPDBStoreTableCacheLoadPOp::onTupleSetWaitRemote(const TupleSetWaitRemoteMessage &msg) {
  // make flight client and connect
  arrow::flight::Location clientLocation;
  auto status = arrow::flight::Location::ForGrpcTcp(msg.getHost(), msg.getPort(), &clientLocation);
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

  // make request
  auto ticketObj = fpdb::store::server::flight::GetTableTicket::make(queryId_, msg.sender(), name_, true);
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }
  auto ticket = *expTicket;

  // load table until receive the "end table"
  while (true) {
    std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
    status = client->DoGet(ticket, &reader);
    if (!status.ok()) {
      ctx()->notifyError(status.message());
      return;
    }
    std::shared_ptr<::arrow::Table> table;
    status = reader->ReadAll(&table);
    if (!status.ok()) {
      ctx()->notifyError(status.message());
      return;
    }

    // check table
    if (table == nullptr) {
      ctx()->notifyError(fmt::format("Received null table from remote node: {}", msg.getHost()));
      return;
    }
    if (fpdb::store::server::flight::Util::isEndTable(table)) {
      ctx()->notifyComplete();
      return;
    } else {
      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(TupleSet::make(table), name_);
      ctx()->tell(tupleSetMessage);
    }
  }
}

void FPDBStoreTableCacheLoadPOp::clear() {
  // Noop
}

}
