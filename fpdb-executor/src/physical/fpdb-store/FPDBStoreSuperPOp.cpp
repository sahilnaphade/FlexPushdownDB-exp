//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/store/server/flight/SelectObjectContentTicket.hpp>
#include <arrow/flight/api.h>

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
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

void FPDBStoreSuperPOp::clear() {
  subPlan_.reset();
}

std::string FPDBStoreSuperPOp::getTypeString() const {
  return "FPDBStoreSuperPOp";
}

void FPDBStoreSuperPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  processAtStore();
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

  // send output tupleSet and complete
  std::shared_ptr<TupleSet> tupleSet;
  if (table->num_rows() > 0) {
    tupleSet = TupleSet::make(table);
  } else {
    tupleSet = TupleSet::make(table->schema());
  }
  std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(tupleSet, name_);
  ctx()->tell(tupleMessage);
  ctx()->notifyComplete();
}

tl::expected<std::string, std::string> FPDBStoreSuperPOp::serialize(bool pretty) {
  PhysicalPlanSerializer serializer(subPlan_);
  return serializer.serialize(pretty);
}

}