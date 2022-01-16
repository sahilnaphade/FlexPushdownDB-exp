//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H

#include <normal/executor/physical/file/FileScanKernel.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/ScanMessage.h>
#include <normal/executor/message/cache/LoadResponseMessage.h>
#include <string>

using namespace normal::executor::message;
using namespace normal::tuple;

namespace normal::executor::physical::file {

class FileScanPOp : public PhysicalOp {

public:
  FileScanPOp(std::string name,
		   const std::string &filePath,
		   FileType fileType,
		   std::vector<std::string> columnNames,
		   unsigned long startOffset,
		   unsigned long finishOffset,
		   bool scanOnStart = false);
  FileScanPOp() = default;
  FileScanPOp(const FileScanPOp&) = default;
  FileScanPOp& operator=(const FileScanPOp&) = default;

  [[nodiscard]] const FileScanKernel &getKernel() const;
  [[nodiscard]] bool isScanOnStart() const;

  void onReceive(const Envelope &message) override;

private:
  void onStart();
  void onCacheLoadResponse(const ScanMessage &Message);
  void onComplete(const CompleteMessage &message);
  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);

  bool scanOnStart_;
  FileScanKernel kernel_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FileScanPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("kernel", op.kernel_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
