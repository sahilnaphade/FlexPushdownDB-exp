//
// Created by matt on 5/12/19.
//

#include <normal/executor/physical/collate/CollatePOp.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>
#include <vector>                      // for vector

namespace normal::executor::physical::collate {

void CollatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // FIXME: Not the best way to reset the tuples structure
  this->tuples_.reset();
}

CollatePOp::CollatePOp(std::string name,
                       std::vector<std::string> projectColumnNames,
                       long queryId) :
  PhysicalOp(std::move(name), "Collate", std::move(projectColumnNames), queryId) {
}

void CollatePOp::onReceive(const normal::executor::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::executor::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::executor::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void CollatePOp::onComplete(const normal::executor::message::CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    if (!tables_.empty()) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok())
        abort();
      tuples_->table(*res);
      tables_.clear();
    }
	ctx()->notifyComplete();
  }
}

void CollatePOp::show() {
  assert(tuples_);
  SPDLOG_DEBUG("Collated  |  tupleSet:\n{}", this->name(), tuples_->toString());
}

std::shared_ptr<TupleSet> CollatePOp::tuples() {
  assert(tuples_);
  return tuples_;
}
void CollatePOp::onTuple(const normal::executor::message::TupleMessage &message) {
  if (!tuples_) {
    assert(message.tuples());
    tuples_ = message.tuples();
  } else {
    tables_.push_back(message.tuples()->table());
    if (tables_.size() > tablesCutoff_) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok())
        abort();
      tuples_->table(*res);
      tables_.clear();
    }
  }
}

[[maybe_unused]] void CollatePOp::setTuples(const std::shared_ptr<TupleSet> &Tuples) {
  tuples_ = Tuples;
}

}