//
// Created by matt on 12/12/19.
//

#include <fpdb/executor/physical/file/FileScanPOp.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/cache/CacheHelper.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <fpdb/tuple/TupleSet.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>

using namespace fpdb::executor::physical::cache;
using namespace fpdb::executor::message;
using namespace fpdb::tuple;

namespace arrow { class MemoryPool; }

namespace fpdb::executor::physical::file {

FileScanPOp::FileScanPOp(const std::string &name,
                         const std::vector<std::string> &columnNames,
                         int nodeId,
                         const std::string &bucket,
                         const std::string &object,
                         const std::string &storeRootPath,
                         const std::shared_ptr<FileFormat> &format,
                         const std::shared_ptr<::arrow::Schema> &schema,
                         const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                         bool scanOnStart) :
	PhysicalOp(name, FILE_SCAN, columnNames, nodeId),
	scanOnStart_(scanOnStart),
	kernel_(FileScanKernel::make(bucket, object, storeRootPath, format, schema, byteRange)){}

std::string FileScanPOp::getTypeString() const {
  return "FileScanPOp";
}

void FileScanPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
	  this->onStart();
  } else if (message.message().type() == MessageType::SCAN) {
    auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
    this->onCacheLoadResponse(scanMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
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
   * TODO: support reading the file in pieces
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
  auto partition = std::make_shared<catalogue::local_fs::LocalFSPartition>(kernel_.getFilePath());
  std::pair<int64_t, int64_t> byteRange;
  auto optByteRange = kernel_.getByteRange();
  if (optByteRange.has_value()) {
    byteRange = *optByteRange;
  } else {
    auto expFileSize = kernel_.getFileSize();
    if (!expFileSize.has_value()) {
      ctx()->notifyError(expFileSize.error());
    }
    byteRange = {0, *expFileSize};
  }

  CacheHelper::requestStoreSegmentsInCache(tupleSet,
                                           partition,
                                           byteRange.first,
                                           byteRange.second,
                                           name(),
                                           ctx());
}

bool FileScanPOp::isScanOnStart() const {
  return scanOnStart_;
}

const FileScanKernel &FileScanPOp::getKernel() const {
  return kernel_;
}

void FileScanPOp::clear() {
  // Noop
}

}