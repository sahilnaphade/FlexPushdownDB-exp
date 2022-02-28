//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/LocalFileScanKernel.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>

namespace fpdb::executor::physical::file {

LocalFileScanKernel::LocalFileScanKernel(const std::shared_ptr<FileFormat> &format,
                                         const std::shared_ptr<::arrow::Schema> &schema,
                                         const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                         const std::string &path) :
  FileScanKernel(CatalogueEntryType::LOCAL_FS, format, schema, byteRange),
  path_(path) {}

std::shared_ptr<LocalFileScanKernel>
LocalFileScanKernel::make(const std::shared_ptr<FileFormat> &format,
                          const std::shared_ptr<::arrow::Schema> &schema,
                          const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                          const std::string &path) {
  return std::make_shared<LocalFileScanKernel>(format, schema, byteRange, path);
}

const std::string &LocalFileScanKernel::getPath() const {
  return path_;
}

void LocalFileScanKernel::setPath(const std::string &path) {
  path_ = path;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> LocalFileScanKernel::scan() {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, path_);
  if (byteRange_.has_value()) {
    return reader->readRange(byteRange_->first, byteRange_->second);
  } else {
    return reader->read();
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalFileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, path_);
  if (byteRange_.has_value()) {
    return reader->readRange(columnNames, byteRange_->first, byteRange_->second);
  } else {
    return reader->read(columnNames);
  }
}

tl::expected<int64_t, std::string> LocalFileScanKernel::getFileSize() const {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, path_);
  return reader->getFileSize();
}

}
