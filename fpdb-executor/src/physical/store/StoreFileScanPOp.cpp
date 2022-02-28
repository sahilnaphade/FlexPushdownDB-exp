//
// Created by Yifei Yang on 2/27/22.
//

#include <fpdb/executor/physical/store/StoreFileScanPOp.h>
#include <fpdb/executor/physical/file/LocalFileScanKernel.h>

namespace fpdb::executor::physical::store {

StoreFileScanPOp::StoreFileScanPOp(const std::string &name,
                                   const std::vector<std::string> &columnNames,
                                   int nodeId,
                                   const std::string &bucket,
                                   const std::string &object,
                                   const std::shared_ptr<FileFormat> &format,
                                   const std::shared_ptr<::arrow::Schema> &schema,
                                   const std::optional<std::pair<int64_t, int64_t>> &byteRange):
  FileScanAbstractPOp(name,
                      STORE_FILE_SCAN,
                      columnNames,
                      nodeId,
                      file::LocalFileScanKernel::make(format, schema, byteRange, ""),
                      true),
  bucket_(bucket),
  object_(object) {}

StoreFileScanPOp::StoreFileScanPOp(const std::string &name,
                                   const std::vector<std::string> &columnNames,
                                   int nodeId,
                                   const std::string &storeRootPath,
                                   const std::string &bucket,
                                   const std::string &object,
                                   const std::shared_ptr<FileFormat> &format,
                                   const std::shared_ptr<::arrow::Schema> &schema,
                                   const std::optional<std::pair<int64_t, int64_t>> &byteRange):
  FileScanAbstractPOp(name,
                      STORE_FILE_SCAN,
                      columnNames,
                      nodeId,
                      file::LocalFileScanKernel::make(format, schema, byteRange,
                                                      fmt::format("{}/{}/{}", storeRootPath, bucket, object)),
                      true),
  bucket_(bucket),
  object_(object) {}

const std::string &StoreFileScanPOp::getBucket() const {
  return bucket_;
}

const std::string &StoreFileScanPOp::getObject() const {
  return object_;
}

std::string StoreFileScanPOp::getTypeString() const {
  return "StoreFileScanPOp";
}

}
