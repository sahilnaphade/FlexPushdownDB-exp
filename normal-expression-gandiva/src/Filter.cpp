//
// Created by matt on 6/5/20.
//

#include <normal/expression/gandiva/Filter.h>
#include <normal/expression/gandiva/Globals.h>
#include <normal/tuple/Globals.h>
#include <normal/tuple/Util.h>
#include <gandiva/tree_expr_builder.h>
#include <utility>

using namespace normal::expression::gandiva;

Filter::Filter(std::shared_ptr<Expression> Pred) : pred_(std::move(Pred)) {}

std::shared_ptr<Filter> Filter::make(const std::shared_ptr<Expression> &Pred) {
  return std::make_shared<Filter>(Pred);
}

tl::expected<arrow::ArrayVector, std::string>
Filter::evaluateBySelectionVectorStatic(const arrow::RecordBatch &recordBatch,
                                        const std::shared_ptr<::gandiva::SelectionVector> &selectionVector) {
  // Build a projector for the pass through expression
  std::shared_ptr<::gandiva::Projector> gandivaProjector;
  std::vector<std::shared_ptr<::gandiva::Expression>> fieldExpressions;
  for (const auto &field: recordBatch.schema()->fields()) {
    auto gandivaField = ::gandiva::TreeExprBuilder::MakeField(field);
    auto fieldExpression = ::gandiva::TreeExprBuilder::MakeExpression(gandivaField, field);
    fieldExpressions.push_back(fieldExpression);
  }
  auto status = ::gandiva::Projector::Make(recordBatch.schema(),
                                           fieldExpressions,
                                           selectionVector->GetMode(),
                                           ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                           &gandivaProjector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // Evaluate the expressions
  /**
   * NOTE: Gandiva fails if the projector is evaluated using an empty selection vector, so need to test for it
   */
  arrow::ArrayVector outputs;
  if (selectionVector->GetNumSlots() > 0) {
    status = gandivaProjector->Evaluate(recordBatch, selectionVector.get(), arrow::default_memory_pool(), &outputs);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  }
  else{
    for (const auto &field: recordBatch.schema()->fields()) {
      const auto &expArray = normal::tuple::Util::makeEmptyArray(field->type());
      if (!expArray) {
        return tl::make_unexpected(expArray.error());
      }
      outputs.emplace_back(*expArray);
    }
  }
  return outputs;
}

tl::expected<arrow::ArrayVector, std::string> Filter::evaluate(const arrow::RecordBatch &recordBatch) {
  assert(recordBatch.ValidateFull().ok());

  // Create a bit vector
  const auto &expSelectionVector = computeSelectionVector(recordBatch);
  if (!expSelectionVector.has_value()) {
    return tl::make_unexpected(expSelectionVector.error());
  }
  const auto &selectionVector = *expSelectionVector;

  SPDLOG_DEBUG("Evaluated SelectionVector  |  vector: {}", selectionVector->ToArray()->ToString());

  // Evaluate the expressions
  return evaluateBySelectionVector(recordBatch, selectionVector);
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet>, std::string>
Filter::evaluate(const normal::tuple::TupleSet &tupleSet) {
  if (tupleSet.valid()) {

    auto filteredTupleSet = normal::tuple::TupleSet::make(tupleSet.schema());
    auto arrowTable = tupleSet.table();

    assert(arrowTable->ValidateFull().ok());

    arrow::Status arrowStatus;

    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::TableBatchReader reader(*arrowTable);
    // Maximum chunk size Gandiva filter evaluates at a time
    reader.set_chunksize((int64_t) normal::tuple::DefaultChunkSize);
    arrowStatus = reader.ReadNext(&batch);
    if (!arrowStatus.ok()) {
      return tl::make_unexpected(arrowStatus.message());
    }

    while (batch != nullptr) {

      assert(batch->ValidateFull().ok());
      std::shared_ptr<::gandiva::SelectionVector> selection_vector;
      auto status = ::gandiva::SelectionVector::MakeInt64(batch->num_rows(), ::arrow::default_memory_pool(), &selection_vector);
      if (!status.ok()) {
        return tl::make_unexpected(status.message());
      }

      status = gandivaFilter_->Evaluate(*batch, selection_vector);

      if (!status.ok()) {
        return tl::make_unexpected(status.message());
      }

      SPDLOG_DEBUG("Evaluated SelectionVector  |  vector: {}", selection_vector->ToArray()->ToString());

      // Evaluate the expressions
      std::shared_ptr<::arrow::Table> batchArrowTable;

      /**
       * NOTE: Gandiva fails if the projector is evaluated using an empty selection vector, so need to test for it
       */
      if(selection_vector->GetNumSlots() > 0) {
        arrow::ArrayVector outputs;
        status = gandivaProjector_->Evaluate(*batch, selection_vector.get(), arrow::default_memory_pool(), &outputs);

        if (!status.ok()) {
          return tl::make_unexpected(status.message());
        }

        batchArrowTable = ::arrow::Table::Make(batch->schema(), outputs);
      }
      else{
        auto columns = Schema::make(batch->schema())->makeColumns();
        auto arrowArrays = Column::columnVectorToArrowChunkedArrayVector(columns);
        batchArrowTable = ::arrow::Table::Make(batch->schema(), arrowArrays);
      }

      auto batchTupleSet = std::make_shared<normal::tuple::TupleSet>(batchArrowTable);

      SPDLOG_DEBUG("Filtered batch:\n{}",
             batchTupleSet->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));

      auto result = filteredTupleSet->append(batchTupleSet);
      if(!result.has_value()){
        return tl::make_unexpected(result.error());
      }

      arrowStatus = reader.ReadNext(&batch);
      if (!arrowStatus.ok()) {
        return tl::make_unexpected(arrowStatus.message());
      }
    }

    return filteredTupleSet;
  }

  else {
    return tl::make_unexpected("Cannot filter tupleSet. TupleSet is invalid.");
  }
}

tl::expected<std::shared_ptr<::gandiva::SelectionVector>, std::string>
Filter::computeSelectionVector(const arrow::RecordBatch &recordBatch) {
  // Create a bit vector
  std::shared_ptr<::gandiva::SelectionVector> selectionVector;
  auto status = ::gandiva::SelectionVector::MakeInt64(recordBatch.num_rows(),
                                                      ::arrow::default_memory_pool(),
                                                      &selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // Compute the bit vector
  status = gandivaFilter_->Evaluate(recordBatch, selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  return selectionVector;
}

tl::expected<arrow::ArrayVector, std::string>
Filter::evaluateBySelectionVector(const arrow::RecordBatch &recordBatch,
                                  const std::shared_ptr<::gandiva::SelectionVector> &selectionVector) {
  /**
   * NOTE: Gandiva fails if the projector is evaluated using an empty selection vector, so need to test for it
   */
  arrow::ArrayVector outputs;
  if(selectionVector->GetNumSlots() > 0) {
    auto status = gandivaProjector_->Evaluate(recordBatch, selectionVector.get(), arrow::default_memory_pool(), &outputs);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  }
  else{
    for (const auto &field: recordBatch.schema()->fields()) {
      const auto &expArray = normal::tuple::Util::makeEmptyArray(field->type());
      if (!expArray) {
        return tl::make_unexpected(expArray.error());
      }
      outputs.emplace_back(*expArray);
    }
  }
  return outputs;
}

tl::expected<void, std::string> Filter::compile(const std::shared_ptr<normal::tuple::Schema> &Schema) {
  std::lock_guard<std::mutex> g(BigGlobalLock);

  // Compile the expressions
  pred_->compile(Schema->getSchema());

  auto gandivaCondition = ::gandiva::TreeExprBuilder::MakeCondition(pred_->getGandivaExpression());

  SPDLOG_DEBUG("Filter predicate:\n{}", gandivaCondition->ToString());

  // Build a filter for the predicate.
  auto status = ::gandiva::Filter::Make(Schema->getSchema(),
                                        gandivaCondition,
                                        ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                        &gandivaFilter_);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // Create a pass through expression
  std::vector<std::shared_ptr<::gandiva::Expression>> fieldExpressions;
  for (const auto &field: Schema->fields()) {
    auto gandivaField = ::gandiva::TreeExprBuilder::MakeField(field);
    auto fieldExpression = ::gandiva::TreeExprBuilder::MakeExpression(gandivaField, field);
    fieldExpressions.push_back(fieldExpression);
  }

  // Build a projector for the pass through expression
  status = ::gandiva::Projector::Make(Schema->getSchema(),
                                      fieldExpressions,
                                      ::gandiva::SelectionVector::MODE_UINT64,
                                      ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                      &gandivaProjector_);

  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  return {};
}
