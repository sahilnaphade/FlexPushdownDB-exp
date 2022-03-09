//
// Created by Yifei Yang on 2/26/22.
//

#include <fpdb/executor/physical/file/RemoteFileScanPOp.h>
#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>

namespace fpdb::executor::physical::file {

RemoteFileScanPOp::RemoteFileScanPOp(const std::string &name,
                                     const std::vector<std::string> &columnNames,
                                     int nodeId,
                                     const std::string &bucket,
                                     const std::string &object,
                                     const std::shared_ptr<FileFormat> &format,
                                     const std::shared_ptr<::arrow::Schema> &schema,
                                     const std::string &host,
                                     int port,
                                     const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                     bool scanOnStart):
  FileScanAbstractPOp(name,
                      REMOTE_FILE_SCAN,
                      columnNames,
                      nodeId,
                      RemoteFileScanKernel::make(format, schema, byteRange, bucket, object, host, port),
                      scanOnStart) {}

std::string RemoteFileScanPOp::getTypeString() const {
  return "RemoteFileScanPOp";
}

}