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
  [[deprecated ("Use constructor accepting a byte range")]]
  FileScanPOp(std::string name, const std::string &filePath, long queryId);

  FileScanPOp(std::string name,
		   const std::string &filePath,
		   FileType fileType,
		   std::vector<std::string> columnNames,
		   unsigned long startOffset,
		   unsigned long finishOffset,
		   long queryId,
		   bool scanOnStart = false);

  static std::shared_ptr<FileScanPOp> make(const std::string &name,
										const std::string &filePath,
										const std::vector<std::string> &columnNames,
										unsigned long startOffset,
										unsigned long finishOffset,
										long queryId = 0,
										bool scanOnStart = false);

  static std::shared_ptr<FileScanPOp> make(const std::string &name,
										const std::string &filePath,
										FileType fileType,
										const std::vector<std::string> &columnNames,
										unsigned long startOffset,
										unsigned long finishOffset,
										long queryId = 0,
										bool scanOnStart = false);

  void onReceive(const Envelope &message) override;

private:
  bool scanOnStart_;
  std::vector<std::string> columnNames_;
  std::unique_ptr<FileScanKernel> kernel_;
public:
  [[nodiscard]] const std::unique_ptr<FileScanKernel> &getKernel() const;
  [[nodiscard]] bool isScanOnStart() const;
  [[nodiscard]] const std::vector<std::string> &getColumnNames() const;
private:
  void onStart();

  void onCacheLoadResponse(const ScanMessage &Message);
  void onComplete(const CompleteMessage &message);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
