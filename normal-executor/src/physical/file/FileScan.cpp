//
// Created by matt on 12/12/19.
//

#include <normal/executor/physical/file/FileScan.h>
#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/cache/CacheHelper.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/catalogue/local-fs/LocalFSPartition.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/csv/CSVParser.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>

using namespace normal::executor::physical::cache;
using namespace normal::executor::message;
using namespace normal::tuple;
using namespace normal::tuple::csv;

namespace arrow { class MemoryPool; }

namespace normal::executor::physical::file {

FileScan::FileScan(std::string name, const std::string& filePath, long queryId) :
	PhysicalOp(std::move(name), "FileScan", queryId),
	scanOnStart_(true),
	kernel_(FileScanKernel::make(filePath, FileType::CSV, 0, ULONG_MAX)){}

FileScan::FileScan(std::string name,
				   const std::string& filePath,
				   FileType fileType,
				   std::vector<std::string>  columnNames,
				   unsigned long startOffset,
				   unsigned long finishOffset,
				   long queryId,
				   bool scanOnStart) :
	PhysicalOp(std::move(name), "FileScan", queryId),
	scanOnStart_(scanOnStart),
	columnNames_(std::move(columnNames)),
	kernel_(FileScanKernel::make(filePath, fileType, startOffset, finishOffset)){}

std::shared_ptr<FileScan> FileScan::make(const std::string& name,
										 const std::string& filePath,
										 const std::vector<std::string>& columnNames,
										 unsigned long startOffset,
										 unsigned long finishOffset,
                     long queryId,
										 bool scanOnStart) {

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);

  return std::make_shared<FileScan>(name,
									filePath,
									FileType::CSV,
									canonicalColumnNames,
									startOffset,
									finishOffset,
									queryId,
									scanOnStart);
}

std::shared_ptr<FileScan> FileScan::make(const std::string& name,
										 const std::string& filePath,
										 FileType fileType,
										 const std::vector<std::string>& columnNames,
										 unsigned long startOffset,
										 unsigned long finishOffset,
                     long queryId,
                     bool scanOnStart) {

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);

  return std::make_shared<FileScan>(name,
									filePath,
									fileType,
									canonicalColumnNames,
									startOffset,
									finishOffset,
									scanOnStart,
									queryId);
}

void FileScan::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "ScanMessage") {
	auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
	this->onCacheLoadResponse(scanMessage);
  }
  else if (message.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
	this->onComplete(completeMessage);
  }
  else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void FileScan::onComplete(const CompleteMessage &) {
  if(ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
	ctx()->notifyComplete();
  }
}

void FileScan::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if(scanOnStart_){
	readAndSendTuples(columnNames_);
	ctx()->notifyComplete();
  }
}

void FileScan::readAndSendTuples(const std::vector<std::string> &columnNames){
  // Read the columns not present in the cache
  /*
   * FIXME: Should support reading the file in pieces
   */

  std::shared_ptr<TupleSet2> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet2::make2();
  } else {
    auto expectedReadTupleSet = kernel_->scan(columnNames);
    readTupleSet = expectedReadTupleSet.value();

    // Store the read columns in the cache
    requestStoreSegmentsInCache(readTupleSet);
  }

  std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);
}

void FileScan::onCacheLoadResponse(const ScanMessage &Message) {
  readAndSendTuples(Message.getColumnNames());
}

void FileScan::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<catalogue::local_fs::LocalFSPartition>(kernel_->getPath());
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, kernel_->getStartPos(), kernel_->getFinishPos(), name(), ctx());
}

bool FileScan::isScanOnStart() const {
  return scanOnStart_;
}
const std::vector<std::string> &FileScan::getColumnNames() const {
  return columnNames_;
}
const std::unique_ptr<FileScanKernel> &FileScan::getKernel() const {
  return kernel_;
}

}