//
// Created by matt on 14/4/20.
//

#include <normal/executor/physical/project/ProjectPOp.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/expression/gandiva/Expression.h>
#include <utility>

namespace normal::executor::physical::project {

ProjectPOp::ProjectPOp(const std::string &name,
                 std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> exprs,
                 std::vector<std::string> exprNames,
                 long queryId)
    : PhysicalOp(name, "Project", queryId),
      exprs_(std::move(exprs)),
      exprNames_(std::move(exprNames)) {}

void ProjectPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void ProjectPOp::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void ProjectPOp::onTuple(const TupleMessage &message) {
  // Build the project if not built
  buildProjector(message);

  // Add the tuples to the internal buffer
  bufferTuples(message);

  // Project and send if the buffer is full enough
  if (tuples_->numRows() > DefaultBufferSize) {
    projectAndSendTuples();
  }
}

void ProjectPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    // Project and send any remaining tuples
    projectAndSendTuples();
    ctx()->notifyComplete();
  }
}

void ProjectPOp::buildProjector(const TupleMessage &message) {
  if(!projector_.has_value()){
    const auto &inputSchema = message.tuples()->table()->schema();
    projector_ = std::make_shared<normal::expression::gandiva::Projector>(exprs_);
    projector_.value()->compile(inputSchema);
  }
}

void ProjectPOp::bufferTuples(const TupleMessage &message) {
  if (!tuples_) {
    // Initialise tuples buffer with message contents
    tuples_ = message.tuples();
  } else {
    // Append message contents to tuples buffer
    auto tables = {tuples_->table(), message.tuples()->table()};
    auto res = arrow::ConcatenateTables(tables);
    if (!res.ok()) {
      tuples_->table(*res);
    } else {
      // FIXME: Propagate error properly
      throw std::runtime_error(res.status().message());
    }
  }
}

void ProjectPOp::projectAndSendTuples() {
  if(tuples_ && tuples_->numRows() > 0) {
    // Project expressions
    auto projectedTuples = projector_.value()->evaluate(*tuples_);

    // TODO: add project columns
//    std::vector<std::shared_ptr<arrow::Field>> fields;
//    std::vector<std::shared_ptr<arrow::ChunkedArray>> arrowColumns;
//    for (auto const &expression: exprs_) {
//      auto columnName = std::static_pointer_cast<normal::expression::gandiva::Column>(expression)->getColumnName();
//      auto arrowColumn = tuples_->table()->GetColumnByName(columnName);
//      arrowColumns.emplace_back(arrowColumn);
//      fields.emplace_back(std::make_shared<arrow::Field>(columnName, arrowColumn->type()));
//    }
//    auto projectedTuples = TupleSet::make(std::make_shared<arrow::Schema>(fields), arrowColumns);

    sendTuples(projectedTuples);

    // FIXME: Either set tuples to size 0 or use an optional
    tuples_ = nullptr;
  }
}

void ProjectPOp::sendTuples(std::shared_ptr<TupleSet> &projected) {
	std::shared_ptr<Message>
		tupleMessage = std::make_shared<TupleMessage>(projected, name());
	ctx()->tell(tupleMessage);
}

}
