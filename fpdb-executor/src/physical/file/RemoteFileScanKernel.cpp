//
// Created by Yifei Yang on 2/27/22.
//

#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>
#include <fpdb/store/server/file/RemoteFileReaderBuilder.h>

using namespace fpdb::store::server::file;

namespace fpdb::executor::physical::file {

RemoteFileScanKernel::RemoteFileScanKernel(const std::shared_ptr<FileFormat> &format,
                                           const std::shared_ptr<::arrow::Schema> &schema,
                                           const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                           const std::string &bucket,
                                           const std::string &object,
                                           const std::string &host,
                                           int port):
  FileScanKernel(CatalogueEntryType::OBJ_STORE, format, schema, byteRange),
  bucket_(bucket),
  object_(object),
  host_(host),
  port_(port) {}

std::shared_ptr<RemoteFileScanKernel>
RemoteFileScanKernel::make(const std::shared_ptr<FileFormat> &format,
                           const std::shared_ptr<::arrow::Schema> &schema,
                           const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                           const std::string &bucket,
                           const std::string &object,
                           const std::string &host,
                           int port) {
  return std::make_shared<RemoteFileScanKernel>(format, schema, byteRange, bucket, object, host, port);
}

const std::string &RemoteFileScanKernel::getBucket() const {
  return bucket_;
}

const std::string &RemoteFileScanKernel::getObject() const {
  return object_;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> RemoteFileScanKernel::scan() {
  auto reader = RemoteFileReaderBuilder::make(format_, schema_, bucket_, object_, host_, port_);
  if (byteRange_.has_value()) {
    return reader->readRange(byteRange_->first, byteRange_->second);
  } else {
    return reader->read();
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteFileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = RemoteFileReaderBuilder::make(format_, schema_, bucket_, object_, host_, port_);
  if (byteRange_.has_value()) {
    return reader->readRange(columnNames, byteRange_->first, byteRange_->second);
  } else {
    return reader->read(columnNames);
  }
}

tl::expected<int64_t, std::string> RemoteFileScanKernel::getFileSize() const {
  auto reader = RemoteFileReaderBuilder::make(format_, schema_, bucket_, object_, host_, port_);
  return reader->getFileSize();
}

}
