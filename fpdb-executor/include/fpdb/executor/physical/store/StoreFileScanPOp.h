//
// Created by Yifei Yang on 2/27/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STOREFILESCANPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STOREFILESCANPOP_H

#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>

namespace fpdb::executor::physical::store {

/**
 * Scan operator executed at storage, during pushdown processing
 */
class StoreFileScanPOp: public file::FileScanAbstractPOp {

public:
  /**
   * Called at compute nodes (client side), specifying bucket and object
   */
  StoreFileScanPOp(const std::string &name,
                   const std::vector<std::string> &columnNames,
                   int nodeId,
                   const std::string &bucket,
                   const std::string &object,
                   const std::shared_ptr<FileFormat> &format,
                   const std::shared_ptr<::arrow::Schema> &schema,
                   const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt);

  /**
   * Called at storage nodes (server side), specifying bucket, object, and storeRootPath
   */
  StoreFileScanPOp(const std::string &name,
                   const std::vector<std::string> &columnNames,
                   int nodeId,
                   const std::string &storeRootPath,
                   const std::string &bucket,
                   const std::string &object,
                   const std::shared_ptr<FileFormat> &format,
                   const std::shared_ptr<::arrow::Schema> &schema,
                   const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt);

  StoreFileScanPOp() = default;
  StoreFileScanPOp(const StoreFileScanPOp&) = default;
  StoreFileScanPOp& operator=(const StoreFileScanPOp&) = default;
  ~StoreFileScanPOp() = default;

  const std::string &getBucket() const;
  const std::string &getObject() const;

  std::string getTypeString() const override;

private:
  std::string bucket_;
  std::string object_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StoreFileScanPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("kernel", op.kernel_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("bucket", op.bucket_),
                               f.field("object", op.object_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_STORE_STOREFILESCANPOP_H
