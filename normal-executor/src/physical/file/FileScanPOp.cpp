//
// Created by matt on 12/12/19.
//

#include <normal/executor/physical/file/FileScanPOp.h>
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

FileScanPOp::FileScanPOp(std::string name,
           std::vector<std::string> columnNames,
           int nodeId,
				   const std::string& filePath,
				   FileType fileType,
				   unsigned long startOffset,
				   unsigned long finishOffset,
				   bool scanOnStart) :
	PhysicalOp(std::move(name), FILE_SCAN, std::move(columnNames), nodeId),
	scanOnStart_(scanOnStart),
	kernel_(FileScanKernel::make(filePath, fileType, startOffset, finishOffset)){}

std::string FileScanPOp::getTypeString() const {
  return "FileScanPOp";
}

void FileScanPOp::onReceive(const Envelope &message) {
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

void FileScanPOp::onComplete(const CompleteMessage &) {
  if(ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
	ctx()->notifyComplete();
  }
}

void FileScanPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if(scanOnStart_){
	readAndSendTuples(getProjectColumnNames());
	ctx()->notifyComplete();
  }
}

void FileScanPOp::readAndSendTuples(const std::vector<std::string> &columnNames){
  // Read the columns not present in the cache
  /*
   * FIXME: Should support reading the file in pieces
   */

  std::shared_ptr<TupleSet> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet::makeWithEmptyTable();
  } else {
    auto expectedReadTupleSet = kernel_.scan(columnNames);
    readTupleSet = expectedReadTupleSet.value();

    // Store the read columns in the cache
    requestStoreSegmentsInCache(readTupleSet);
  }

  std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet, this->name());
  ctx()->tell(message);
}

void FileScanPOp::onCacheLoadResponse(const ScanMessage &Message) {
  readAndSendTuples(Message.getColumnNames());
}

void FileScanPOp::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet) {
  auto partition = std::make_shared<catalogue::local_fs::LocalFSPartition>(kernel_.getPath());
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, kernel_.getStartPos(), kernel_.getFinishPos(), name(), ctx());
}

bool FileScanPOp::isScanOnStart() const {
  return scanOnStart_;
}

const FileScanKernel &FileScanPOp::getKernel() const {
  return kernel_;
}

}