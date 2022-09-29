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
    if (!expArray.has_value()) {
      return tl::make_unexpected(expArray.error());
    }
    arrayVec.emplace_back(*expArray);
  }
  return arrow::RecordBatch::Make(schema, 0, arrayVec);
}

tl::expected<::arrow::RecordBatchVector, std::string>
Util::table_to_record_batches(const std::shared_ptr<arrow::Table> &table) {
  if (table == nullptr) {
    return tl::make_unexpected("Cannot make record batches from null table");
  }

  ::arrow::RecordBatchVector batches;
  if (table->num_rows() > 0) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ::arrow::TableBatchReader tbl_reader(*table);
    tbl_reader.set_chunksize(fpdb::tuple::DefaultChunkSize);
    auto status = tbl_reader.ReadAll(&batches);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }
  } else {
    auto expRecordBatch = makeEmptyRecordBatch(table->schema());
    if (!expRecordBatch.has_value()) {
      return tl::make_unexpected(expRecordBatch.error());
    }
    batches.emplace_back(*expRecordBatch);
  }
  return batches;
}


}
