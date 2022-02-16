//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/FileReader.h>
#include <arrow/io/api.h>

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

void FileReader::close(const std::shared_ptr<arrow::io::ReadableFile> &inputStream) {
  if (inputStream && !inputStream->closed()) {
    auto status = inputStream->Close();
    if (!status.ok()) {
      SPDLOG_WARN("Close input stream failed: {}", status.message());
    }
  }
}

}
