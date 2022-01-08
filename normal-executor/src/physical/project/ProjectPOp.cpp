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

ProjectPOp::ProjectPOp(std::string name,
                 std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> exprs,
                 std::vector<std::string> exprNames,
                 std::vector<std::pair<std::string, std::string>> projectColumnNamePairs,
                 std::vector<std::string> projectColumnNames)
    : PhysicalOp(std::move(name), "ProjectPOp", std::move(projectColumnNames)),
      exprs_(std::move(exprs)),
      exprNames_(std::move(exprNames)),
      projectColumnNamePairs_(std::move(projectColumnNamePairs)) {}

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
  if(!projector_.has_value() && !exprs_.empty()){
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
      throw std::runtime_error(res.status().message());
    }
  }
}

void ProjectPOp::projectAndSendTuples() {
  if(tuples_ && tuples_->numRows() > 0) {
    // Collect project columns and project expr columns
    std::vector<std::shared_ptr<Column>> columns;

    // Project columns
    for (const auto &projectColumnPair: projectColumnNamePairs_) {
      const auto &inputColumnName = projectColumnPair.first;
      const auto &outputColumnName = projectColumnPair.second;
      const auto &expColumn = tuples_->getColumnByName(inputColumnName);
      if (!expColumn.has_value()) {
        throw std::runtime_error(expColumn.error());
      }
      const auto &column = expColumn.value();
      column->setName(outputColumnName);
      columns.emplace_back(column);
    }

    // Columns from project exprs
    if (!exprs_.empty()) {
      // Evaluate
      auto projExprTuples = projector_.value()->evaluate(*tuples_);

      // Rename
      auto renameRes = projExprTuples->renameColumns(exprNames_);
      if (!renameRes.has_value()) {
        throw std::runtime_error(renameRes.error());
      }

      // Add columns
      for (int c = 0; c < projExprTuples->numColumns(); ++c) {
        const auto &expColumn = projExprTuples->getColumnByIndex(c);
        if (!expColumn.has_value()) {
          throw std::runtime_error(expColumn.error());
        }
        columns.emplace_back(expColumn.value());
      }
    }

    std::shared_ptr<TupleSet> fullTupleSet = TupleSet::make(columns);

    // Project the fullTupleSet using projectColumnNames
    auto expProjectTupleSet = fullTupleSet->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      throw std::runtime_error(expProjectTupleSet.error());
    }

    // Send
    sendTuples(expProjectTupleSet.value());
    tuples_->clear();
  }
}

void ProjectPOp::sendTuples(std::shared_ptr<TupleSet> &projected) {
	std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(projected, name());
	ctx()->tell(tupleMessage);
}

}
