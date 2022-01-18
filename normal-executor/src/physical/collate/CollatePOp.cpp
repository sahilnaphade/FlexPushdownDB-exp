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

CollatePOp::CollatePOp(std::string name,
                       std::vector<std::string> projectColumnNames,
                       int nodeId) :
  PhysicalOp(std::move(name), COLLATE, std::move(projectColumnNames), nodeId) {
}

std::string CollatePOp::getTypeString() const {
  return "CollatePOp";
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

void CollatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // FIXME: Not the best way to reset the tuples structure
  this->tuples_.reset();
}

void CollatePOp::onComplete(const normal::executor::message::CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    if (!tables_.empty()) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok()) {
        throw std::runtime_error(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }

    // make the order of output columns the same as the query specifies
    if (tuples_ && tuples_->valid()) {
      const auto &expTupleSet = tuples_->projectExist(getProjectColumnNames());
      if (!expTupleSet.has_value()) {
        throw std::runtime_error(expTupleSet.error());
      }
      tuples_ = expTupleSet.value();
    } else {
      tuples_ = TupleSet::makeWithEmptyTable();
    }

	  ctx()->notifyComplete();
  }
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
      if (!res.ok()) {
        throw std::runtime_error(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }
  }
}

std::shared_ptr<TupleSet> CollatePOp::tuples() {
  assert(tuples_);
  return tuples_;
}

}