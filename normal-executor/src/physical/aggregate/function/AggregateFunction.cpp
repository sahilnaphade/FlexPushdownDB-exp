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

set<string> AggregateFunction::involvedColumnNames() {
  if (expression_) {
    return expression_->involvedColumnNames();
  } else {
    return set<string>();
  }
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

tl::expected<shared_ptr<arrow::Array>, string>
AggregateFunction::buildFinalizeInputArray(const vector<shared_ptr<AggregateResult>> &aggregateResults,
                                           const string &key) {
  // make arrayBuilder
  unique_ptr<arrow::ArrayBuilder> arrayBuilder;
  arrow::Status status = arrow::MakeBuilder(arrow::default_memory_pool(), returnType(), &arrayBuilder);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // append
  for (const auto &aggregateResult: aggregateResults) {
    const auto &expResultScalar = aggregateResult->get(key);
    if (!expResultScalar.has_value()) {
      return tl::make_unexpected(fmt::format("Aggregate result key not found: {}", key));
    }
    status = arrayBuilder->AppendScalar(*expResultScalar.value());
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  }

  // finalize
  const auto &expArray = arrayBuilder->Finish();
  if (!expArray.ok()) {
    return tl::make_unexpected(expArray.status().message());
  }
  return expArray.ValueOrDie();
}

}