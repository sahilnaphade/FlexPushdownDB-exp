//
// Created by Yifei Yang on 1/18/22.
//

#include <fpdb/tuple/util/Util.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>

namespace fpdb::tuple::util {

tl::expected<std::shared_ptr<arrow::Array>, std::string>
Util::makeEmptyArray(const std::shared_ptr<arrow::DataType> &type) {
  auto expAppender = ArrayAppenderBuilder::make(type);
  if (!expAppender.has_value()) {
    return tl::make_unexpected(expAppender.error());
  }
  const auto &appender = *expAppender;

  auto expArray = appender->finalize();
  if (!expArray.has_value()) {
    return tl::make_unexpected(expArray.error());
  }
  return *expArray;
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
Util::makeEmptyRecordBatch(const std::shared_ptr<arrow::Schema> &schema) {
  arrow::ArrayVector arrayVec;
  for (const auto &field: schema->fields()) {
    auto expArray = makeEmptyArray(field->type());
    if (!expArray) {
      return tl::make_unexpected(expArray.error());
    }
    arrayVec.emplace_back(*expArray);
  }
  return arrow::RecordBatch::Make(schema, 0, arrayVec);
}

}
