//
// Created by matt on 30/7/20.
//

#include "ArrayHasher.h"

#include <utility>
#include "StringArrayHasher.h"

ArrayHasher::ArrayHasher(std::shared_ptr<::arrow::Array> array) : array_(std::move(array)) {}

tl::expected<std::shared_ptr<ArrayHasher>, std::string>
ArrayHasher::make(const std::shared_ptr<::arrow::Array> &array) {

  if (array->type_id() == ::arrow::StringType::type_id) {
	return std::make_shared<StringArrayHasher>(array);
  } else {
	return tl::make_unexpected(
		fmt::format("ArrayHasher for type '{}' not implemented yet", array->type_id()));
  }
}
