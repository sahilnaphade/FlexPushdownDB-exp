//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H

#include <fpdb/tuple/FileReader.h>
#include <fpdb/tuple/FileFormat.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/FileReaderBuilder.h>
#include <tl/expected.hpp>
#include <string>
#include <vector>
#include <memory>
#include <optional>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::file {

class FileScanKernel {

public:
  FileScanKernel(const std::string &path,
                 const std::shared_ptr<FileFormat> &format,
                 const std::shared_ptr<::arrow::Schema> &schema,
                 const std::optional<std::pair<int64_t, int64_t>> &byteRange);
  FileScanKernel() = default;
  FileScanKernel(const FileScanKernel&) = default;
  FileScanKernel& operator=(const FileScanKernel&) = default;

  static FileScanKernel make(const std::string &path,
                             const std::shared_ptr<FileFormat> &format,
                             const std::shared_ptr<::arrow::Schema> &schema,
                             const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt);

  tl::expected<std::shared_ptr<TupleSet>, std::string> scan();
  tl::expected<std::shared_ptr<TupleSet>, std::string> scan(const std::vector<std::string> &columnNames);

  const std::string &getPath() const;
  std::pair<int64_t, int64_t> getByteRange() const;

private:
  std::string path_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<::arrow::Schema> schema_;
  std::optional<std::pair<int64_t, int64_t>> byteRange_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FileScanKernel& kernel) {
    auto schemaToBytes = [&kernel]() -> decltype(auto) {
      return fpdb::tuple::ArrowSerializer::schema_to_bytes(kernel.schema_);
    };
    auto schemaFromBytes = [&kernel](const std::vector<std::uint8_t> &bytes) {
      kernel.schema_ = ArrowSerializer::bytes_to_schema(bytes);
      return true;
    };
    return f.object(kernel).fields(f.field("path", kernel.path_),
                                   f.field("format", kernel.format_),
                                   f.field("schema", schemaToBytes, schemaFromBytes),
                                   f.field("byteRange", kernel.byteRange_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
