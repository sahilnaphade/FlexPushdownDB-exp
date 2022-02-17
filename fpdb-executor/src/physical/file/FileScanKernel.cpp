//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/FileScanKernel.h>
#include <fpdb/tuple/FileReaderBuilder.h>

using namespace fpdb::executor::physical::file;

FileScanKernel::FileScanKernel(const std::string &path,
                               const std::shared_ptr<FileFormat> &format,
                               const std::shared_ptr<::arrow::Schema> &schema,
                               const std::optional<std::pair<int64_t, int64_t>> &byteRange) :
	path_(path),
  format_(format),
  schema_(schema),
  byteRange_(byteRange) {}

FileScanKernel FileScanKernel::make(const std::string &path,
                                    const std::shared_ptr<FileFormat> &format,
                                    const std::shared_ptr<::arrow::Schema> &schema,
                                    const std::optional<std::pair<int64_t, int64_t>> &byteRange) {
  return {path, format, schema, byteRange};
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileScanKernel::scan() {
  auto reader = FileReaderBuilder::make(path_, format_, schema_);
  if (byteRange_.has_value()) {
    return reader->read(byteRange_->first, byteRange_->second);
  } else {
    return reader->read();
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
FileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = FileReaderBuilder::make(path_, format_, schema_);
  if (byteRange_.has_value()) {
    return reader->read(columnNames, byteRange_->first, byteRange_->second);
  } else {
    return reader->read(columnNames);
  }
}

const std::string &FileScanKernel::getPath() const {
  return path_;
}

std::pair<int64_t, int64_t> FileScanKernel::getByteRange() const {
  if (byteRange_.has_value()) {
    return *byteRange_;
  } else {
    auto reader = FileReaderBuilder::make(path_, format_, schema_);
    return {0, reader->getFileSize()};
  }
}
