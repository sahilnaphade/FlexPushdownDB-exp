//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/FileScanKernel.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>

using namespace fpdb::executor::physical::file;

FileScanKernel::FileScanKernel(const std::string &bucket,
                               const std::string &object,
                               const std::string &storeRootPath,
                               const std::shared_ptr<FileFormat> &format,
                               const std::shared_ptr<::arrow::Schema> &schema,
                               const std::optional<std::pair<int64_t, int64_t>> &byteRange) :
  bucket_(bucket),
  object_(object),
  storeRootPath_(storeRootPath),
  format_(format),
  schema_(schema),
  byteRange_(byteRange) {}

FileScanKernel FileScanKernel::make(const std::string &bucket,
                                    const std::string &object,
                                    const std::string &storeRootPath,
                                    const std::shared_ptr<FileFormat> &format,
                                    const std::shared_ptr<::arrow::Schema> &schema,
                                    const std::optional<std::pair<int64_t, int64_t>> &byteRange) {
  return {bucket, object, storeRootPath, format, schema, byteRange};
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileScanKernel::scan() {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, getFilePath());
  if (byteRange_.has_value()) {
    return reader->readRange(byteRange_->first, byteRange_->second);
  } else {
    return reader->read();
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
FileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, getFilePath());
  if (byteRange_.has_value()) {
    return reader->readRange(columnNames, byteRange_->first, byteRange_->second);
  } else {
    return reader->read(columnNames);
  }
}

const std::string &FileScanKernel::getBucket() const {
  return bucket_;
}

const std::string &FileScanKernel::getObject() const {
  return object_;
}

const std::shared_ptr<FileFormat> &FileScanKernel::getFormat() const {
  return format_;
}

const std::shared_ptr<::arrow::Schema> &FileScanKernel::getSchema() const {
  return schema_;
}

const std::optional<std::pair<int64_t, int64_t>> &FileScanKernel::getByteRange() const {
  return byteRange_;
}

std::string FileScanKernel::getFilePath() const {
  return fmt::format("{}/{}/{}", storeRootPath_, bucket_, object_);
}

tl::expected<int64_t, std::string> FileScanKernel::getFileSize() const {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, getFilePath());
  return reader->getFileSize();
}
