//
// Created by matt on 8/5/20.
//

#include "normal/tuple/ColumnBuilder.h"

using namespace normal::tuple;

ColumnBuilder::ColumnBuilder(std::string name, const std::shared_ptr<::arrow::DataType> &type) :name_(std::move(name)) {
  auto arrowStatus = ::arrow::MakeBuilder(::arrow::default_memory_pool(), type, &arrowBuilder_);
}

std::shared_ptr<ColumnBuilder> ColumnBuilder::make(const std::string &name,
												   const std::shared_ptr<::arrow::DataType> &type) {
  return std::make_unique<ColumnBuilder>(name, type);
}

void ColumnBuilder::append(const std::shared_ptr<Scalar> &scalar) {
  auto rawBuilderPtr = arrowBuilder_.get();
  if (scalar->type()->id() == ::arrow::Int32Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Int32Builder*>(rawBuilderPtr);
    auto status = typedArrowBuilder->Append(scalar->value<int>());
  }
  else if (scalar->type()->id() == ::arrow::Int64Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Int64Builder*>(rawBuilderPtr);
    auto status = typedArrowBuilder->Append(scalar->value<long>());
  }
  else if (scalar->type()->id() == ::arrow::DoubleType::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::DoubleBuilder*>(rawBuilderPtr);
    auto status = typedArrowBuilder->Append(scalar->value<double>());
  }
  else if (scalar->type()->id() == ::arrow::Date64Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Date64Builder*>(rawBuilderPtr);
    auto status = typedArrowBuilder->Append(scalar->value<long>());
  }
  else if (scalar->type()->id() == ::arrow::StringType::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::StringBuilder*>(rawBuilderPtr);
    auto status = typedArrowBuilder->Append(scalar->value<std::string>());
  }
  else {
    throw std::runtime_error("Builder for type '" + scalar->type()->ToString() + "' not implemented yet");
  }
}

tl::expected<void, std::string> ColumnBuilder::appendNulls(int64_t length) {
  auto status = arrowBuilder_->AppendNulls(length);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  return {};
}

tl::expected<std::shared_ptr<Column>, std::string> ColumnBuilder::finalize() {
  auto status = arrowBuilder_->Finish(&array_);
  if (!status.ok()) {
    return tl::make_unexpected("Error when finalizing ColumnBuilder, " + status.message());
  }
  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(array_);
  return std::make_shared<Column>(name_, chunkedArray);
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> ColumnBuilder::finalizeToArray() {
  auto status = arrowBuilder_->Finish(&array_);
  if (!status.ok()) {
    return tl::make_unexpected("Error when finalizing ColumnBuilder, " + status.message());
  }
  return array_;
}
