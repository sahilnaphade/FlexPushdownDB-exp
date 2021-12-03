//
// Created by matt on 7/3/20.
//

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/expression/gandiva/Column.h>
#include <utility>

namespace normal::executor::physical::aggregate {

AggregateFunction::AggregateFunction(string outputColumnName,
                                     shared_ptr<normal::expression::gandiva::Expression> expression) :
  outputColumnName_(move(outputColumnName)),
  expression_(move(expression)) {}

const string &AggregateFunction::getOutputColumnName() const {
  return outputColumnName_;
}

const shared_ptr<normal::expression::gandiva::Expression> &AggregateFunction::getExpression() const {
  return expression_;
}

shared_ptr<arrow::DataType> AggregateFunction::returnType() {
  return returnType_;
}

shared_ptr<arrow::ChunkedArray> AggregateFunction::evaluateExpr(const shared_ptr<TupleSet> &tupleSet) {
  // just column projection, no need to use gandiva projector
  if (expression_->getType() == expression::gandiva::COLUMN) {
    const auto columnName = static_pointer_cast<expression::gandiva::Column>(expression_)->getColumnName();
    const auto &column = tupleSet->table()->GetColumnByName(columnName);
    returnType_ = column->type();
    return column;
  }

  // expression projection, need to use gandiva projector
  else {
    // Set the input schema and build the projector if not done yet
    cacheInputSchema(tupleSet);
    buildAndCacheProjector();

    const auto exprTupleSet = projector_.value()->evaluate(*tupleSet);
    return exprTupleSet->table()->column(0);
  }
}

void AggregateFunction::cacheInputSchema(const shared_ptr<TupleSet> &tupleSet) {
  if (!inputSchema_.has_value()) {
    inputSchema_ = tupleSet->table()->schema();
  }
}

void AggregateFunction::buildAndCacheProjector() {
  if (!projector_.has_value()) {
    auto expressionsVec = {this->expression_};
    projector_ = std::make_shared<expression::gandiva::Projector>(expressionsVec);
    projector_.value()->compile(inputSchema_.value());
    returnType_ = expression_->getReturnType();
  }
}

}