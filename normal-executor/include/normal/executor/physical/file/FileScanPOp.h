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
		   long queryId = 0,
		   bool scanOnStart = false);

  [[nodiscard]] const std::unique_ptr<FileScanKernel> &getKernel() const;
  [[nodiscard]] bool isScanOnStart() const;

  void onReceive(const Envelope &message) override;

private:
  void onStart();
  void onCacheLoadResponse(const ScanMessage &Message);
  void onComplete(const CompleteMessage &message);
  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);

  bool scanOnStart_;
  std::unique_ptr<FileScanKernel> kernel_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
