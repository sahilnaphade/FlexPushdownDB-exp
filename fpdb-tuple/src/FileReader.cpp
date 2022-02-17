//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/FileReader.h>
#include <arrow/io/api.h>
#include <filesystem>

namespace fpdb::tuple {

FileReader::FileReader(const std::string path,
                       const std::shared_ptr<FileFormat> &format,
                       const std::shared_ptr<::arrow::Schema> schema):
  path_(path),
  format_(format),
  schema_(schema) {}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileReader::read() {
  return read(schema_->field_names());
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileReader::read(int64_t startPos, int64_t finishPos) {
  return read(schema_->field_names(), startPos, finishPos);
}

int64_t FileReader::getFileSize() const {
  std::filesystem::path fsPath(path_);
  return std::filesystem::file_size(fsPath);
}

const std::shared_ptr<FileFormat> &FileReader::getFormat() const {
  return format_;
}

void FileReader::close(const std::shared_ptr<arrow::io::ReadableFile> &inputStream) {
  if (inputStream && !inputStream->closed()) {
    auto status = inputStream->Close();
    if (!status.ok()) {
      SPDLOG_WARN("Close input stream failed: {}", status.message());
    }
  }
}

}
