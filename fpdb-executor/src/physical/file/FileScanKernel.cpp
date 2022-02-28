//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/FileScanKernel.h>

namespace fpdb::executor::physical::file {

FileScanKernel::FileScanKernel(CatalogueEntryType type,
                               const std::shared_ptr<FileFormat> &format,
                               const std::shared_ptr<::arrow::Schema> &schema,
                               const std::optional<std::pair<int64_t, int64_t>> &byteRange) :
  type_(type),
  format_(format),
  schema_(schema),
  byteRange_(byteRange) {}

CatalogueEntryType FileScanKernel::getType() const {
  return type_;
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

}
