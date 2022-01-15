//
// Created by Yifei Yang on 1/12/22.
//

#include <normal/tuple/serialization/ArrowSerializer.h>
#include <fmt/format.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

namespace normal::tuple {

std::shared_ptr<arrow::Table> ArrowSerializer::bytes_to_table(const std::vector<std::uint8_t>& bytes_vec) {
  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(*maybe_buffer);

  // Get a record batch reader over that
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_reader.status().message()));

  // Read the table
  std::shared_ptr<arrow::Table> table;
  status = (*maybe_reader)->ReadAll(&table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", status.message()));

  return table;
}

std::vector<std::uint8_t> ArrowSerializer::table_to_bytes(const std::shared_ptr<arrow::Table>& table) {
  arrow::Status status;

  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), table->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteTable(*table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto data = (*maybe_buffer)->data();
  auto length = (*maybe_buffer)->size();

  std::vector<std::uint8_t> bytes_vec(data, data + length);

  return bytes_vec;
}

std::shared_ptr<arrow::RecordBatch> ArrowSerializer::bytes_to_recordBatch(const std::vector<std::uint8_t>& bytes_vec) {
  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(*maybe_buffer);

  // Get a record batch reader over that
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", maybe_reader.status().message()));

  // Read the table
  std::shared_ptr<arrow::RecordBatch> recordBatch;
  status = (*maybe_reader)->ReadNext(&recordBatch);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow record batch  |  error: {}", status.message()));

  return recordBatch;
}

std::vector<std::uint8_t> ArrowSerializer::recordBatch_to_bytes(const std::shared_ptr<arrow::RecordBatch>& recordBatch) {
  arrow::Status status;

  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), recordBatch->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteRecordBatch(*recordBatch);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow record batch to bytes  |  error: {}", status.message()));

  auto data = (*maybe_buffer)->data();
  auto length = (*maybe_buffer)->size();

  std::vector<std::uint8_t> bytes_vec(data, data + length);

  return bytes_vec;
}

std::shared_ptr<arrow::Schema> ArrowSerializer::bytes_to_schema(const std::vector<std::uint8_t>& bytes_vec) {
  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow schema  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  ::arrow::io::BufferReader buffer_reader(*maybe_buffer);
  ::arrow::ipc::DictionaryMemo dictionaryMemo;

  // Read the schema
  auto expSchema = ::arrow::ipc::ReadSchema(&buffer_reader, &dictionaryMemo);
  if (!expSchema.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow schema  |  error: {}", expSchema.status().message()));

  return *expSchema;
}

std::vector<std::uint8_t> ArrowSerializer::schema_to_bytes(const std::shared_ptr<arrow::Schema>& schema) {
  auto expBuffer = ::arrow::ipc::SerializeSchema(*schema);
  if (!expBuffer.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow schema to bytes  |  error: {}", expBuffer.status().message()));
  }
  const auto &buffer = *expBuffer;

  auto data = buffer->data();
  auto length = buffer->size();
  std::vector<std::uint8_t> bytes_vec(data, data + length);
  return bytes_vec;
}

std::shared_ptr<arrow::ChunkedArray> ArrowSerializer::bytes_to_chunkedArray(const std::vector<std::uint8_t> &bytes_vec) {
  auto table = bytes_to_table(bytes_vec);
  return table->column(0);
}

std::vector<std::uint8_t> ArrowSerializer::chunkedArray_to_bytes(const std::shared_ptr<arrow::ChunkedArray> &chunkedArray) {
  // prepare the Table, as arrow only supports RecordBatch/Table level serialization
  auto field = std::make_shared<arrow::Field>("", chunkedArray->type());
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto columns = std::vector<std::shared_ptr<arrow::ChunkedArray>>{chunkedArray};
  auto table = arrow::Table::Make(schema, columns);

  return table_to_bytes(table);
}

std::shared_ptr<arrow::Scalar> ArrowSerializer::bytes_to_scalar(const std::vector<std::uint8_t>& bytes_vec) {
  auto recordBatch = bytes_to_recordBatch(bytes_vec);
  auto expScalar = recordBatch->column(0)->GetScalar(0);
  if (!expScalar.ok()) {
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow scalar  |  error: {}", expScalar.status().message()));
  }
  return expScalar.ValueOrDie();
}

std::vector<std::uint8_t> ArrowSerializer::scalar_to_bytes(const std::shared_ptr<arrow::Scalar>& scalar) {
  // prepare the RecordBatch
  auto field = std::make_shared<arrow::Field>("", scalar->type);
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{field};
  auto schema = std::make_shared<arrow::Schema>(fields);

  std::unique_ptr<::arrow::ArrayBuilder> arrayBuilder;
  auto status = ::arrow::MakeBuilder(::arrow::default_memory_pool(), scalar->type, &arrayBuilder);
  if (!status.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", status.message()));
  }
  status = arrayBuilder->AppendScalar(*scalar);
  if (!status.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", status.message()));
  }
  auto expArray = arrayBuilder->Finish();
  if (!expArray.ok()) {
    throw std::runtime_error(fmt::format("Error converting Arrow scalar to bytes  |  error: {}", expArray.status().message()));
  }
  const auto &array = expArray.ValueOrDie();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>{array};
  auto recordBatch = ::arrow::RecordBatch::Make(schema, 1, arrays);

  return recordBatch_to_bytes(recordBatch);
}

}
