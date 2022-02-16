//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/parquet/ParquetReader.h>
#include <filesystem>

using namespace fpdb::tuple;

namespace fpdb::tuple::parquet {

ParquetReader::ParquetReader(const std::string &path,
                             const std::shared_ptr<FileFormat> &format,
                             const std::shared_ptr<::arrow::Schema> &schema) :
  FileReader(path, format, schema) {}

std::shared_ptr<ParquetReader> ParquetReader::make(const std::string &path,
                                                   const std::shared_ptr<FileFormat> &format,
                                                   const std::shared_ptr<::arrow::Schema> &schema) {
  return std::make_shared<ParquetReader>(path, format, schema);
}

tl::expected<std::shared_ptr<TupleSet>, std::string> ParquetReader::read(const std::vector<std::string> &columnNames) {
  // open the file
  auto expInFile = ::arrow::io::ReadableFile::Open(path_);
  if (!expInFile.ok()) {
    return tl::make_unexpected(expInFile.status().message());
  }
  auto inFile = *expInFile;

  // create the reader
  std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
  ::arrow::Status status = ::parquet::arrow::OpenFile(inFile,
                                                      ::arrow::default_memory_pool(),
                                                      &parquetReader);
  if (!status.ok()) {
    close(inFile);
    return tl::make_unexpected(status.message());
  }
  parquetReader->set_use_threads(false);

  // read the file
  std::shared_ptr<arrow::Table> table;
  std::vector<int> columnIndices;
  for (const auto &columnName: columnNames) {
    auto columnIndex = schema_->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      close(inFile);
      return tl::make_unexpected(fmt::format("Read Parquet Error: column {} not found", columnName));
    }
    columnIndices.emplace_back(columnIndex);
  }
  status = parquetReader->ReadTable(columnIndices, &table);
  if (!status.ok()) {
    close(inFile);
    return tl::make_unexpected(status.message());
  }

  return TupleSet::make(table);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
ParquetReader::read(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  // open
  ::arrow::Status status;
  auto absolutePath = std::filesystem::absolute(path_);

  auto expectedInputStream = ::arrow::io::ReadableFile::Open(absolutePath);
  if (!expectedInputStream.ok())
    return tl::make_unexpected(expectedInputStream.status().message());
  auto inputStream = *expectedInputStream;

  std::unique_ptr<::parquet::arrow::FileReader> arrowReader;
  status = ::parquet::arrow::OpenFile(inputStream, ::arrow::default_memory_pool(), &arrowReader);
  if (!status.ok()) {
    close(inputStream);
    return tl::make_unexpected(status.message());
  }

  // read
  auto metadata = arrowReader->parquet_reader()->metadata();

  std::vector<int> rowGroupIndexes;
  for (int rowGroupIndex = 0; rowGroupIndex < metadata->num_row_groups(); ++rowGroupIndex) {
    auto rowGroupMetaData = metadata->RowGroup(rowGroupIndex);
    auto columnChunkMetaData = rowGroupMetaData->ColumnChunk(0);
    auto offset = columnChunkMetaData->file_offset();

    if (offset >= startPos && offset <= finishPos) {
      rowGroupIndexes.emplace_back(rowGroupIndex);
    }
  }

  std::unordered_map<std::string, bool> columnIndexMap;
  for (const auto &columnName: columnNames) {
	  columnIndexMap.emplace(ColumnName::canonicalize(columnName), true);
  }

  std::vector<int> columnIndexes;
  for (int columnIndex = 0; columnIndex < metadata->schema()->num_columns(); ++columnIndex) {
    auto columnMetaData = metadata->schema()->Column(columnIndex);
    if (columnIndexMap.find(ColumnName::canonicalize(columnMetaData->name())) != columnIndexMap.end()) {
      columnIndexes.emplace_back(columnIndex);
    }
  }

  if (columnIndexes.empty())
    SPDLOG_WARN(
      "ParquetReader will not read any data. The supplied column names did not match any columns in the file's schema. \n"
      "Parquet File: '{}'\n"
      "Columns: '{}'\n",
      path_,
      fmt::join(columnNames, ","));

  std::unique_ptr<::arrow::RecordBatchReader> recordBatchReader;
  status = arrowReader->GetRecordBatchReader(rowGroupIndexes, columnIndexes, &recordBatchReader);
  if (!status.ok()) {
    close(inputStream);
    return tl::make_unexpected(status.message());
  }

  std::shared_ptr<::arrow::Table> table;
  status = recordBatchReader->ReadAll(&table);
  if (!status.ok()) {
    close(inputStream);
    return tl::make_unexpected(status.message());
  }

  auto tableColumnNames = table->schema()->field_names();
  std::vector<std::string> canonicalColumnNames;
  std::transform(tableColumnNames.begin(), tableColumnNames.end(),
				 std::back_inserter(canonicalColumnNames),
				 [](auto name) -> auto { return ColumnName::canonicalize(name); });

  auto expectedTable = table->RenameColumns(canonicalColumnNames);
  if (expectedTable.ok())
	  table = *expectedTable;
  else {
    close(inputStream);
    return tl::make_unexpected(expectedTable.status().message());
  }
  auto tupleSet = TupleSet::make(table);

  // close and return
  close(inputStream);
  return tupleSet;
}

}
