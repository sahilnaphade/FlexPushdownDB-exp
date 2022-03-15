//
// Created by matt on 12/12/19.
//

#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>
#include <fpdb/executor/physical/file/LocalFileScanKernel.h>
#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>
#include <fpdb/executor/physical/cache/CacheHelper.h>
#include <fpdb/executor/message/DebugMetricsMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <fpdb/catalogue/obj-store/ObjStorePartition.h>

using namespace fpdb::executor::message;
using namespace fpdb::catalogue;

namespace fpdb::executor::physical::file {

FileScanAbstractPOp::FileScanAbstractPOp(const std::string &name,
                                         POpType type,
                                         const std::vector<std::string> &columnNames,
                                         int nodeId,
                                         const std::shared_ptr<FileScanKernel> &kernel,
                                         bool scanOnStart) :
	PhysicalOp(name, type, columnNames, nodeId),
	kernel_(kernel),
  scanOnStart_(scanOnStart) {}

const std::shared_ptr<FileScanKernel> &FileScanAbstractPOp::getKernel() const {
  return kernel_;
}

void FileScanAbstractPOp::onReceive(const Envelope &message) {
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

void FileScanAbstractPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  if(scanOnStart_) {
    // scan
    readAndSendTuples(getProjectColumnNames());

    // metrics
#if SHOW_DEBUG_METRICS == true
    std::shared_ptr<Message> execMetricsMsg =
            std::make_shared<DebugMetricsMessage>(kernel_->getBytesReadRemote(), this->name());
    ctx()->notifyRoot(execMetricsMsg);
#endif

    // complete
    ctx()->notifyComplete();
  }
}

void FileScanAbstractPOp::onComplete(const CompleteMessage &) {
  if(ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    ctx()->notifyComplete();
  }
}

void FileScanAbstractPOp::onCacheLoadResponse(const ScanMessage &Message) {
  readAndSendTuples(Message.getColumnNames());
}

void FileScanAbstractPOp::readAndSendTuples(const std::vector<std::string> &columnNames){
  // Read the columns not present in the cache
  /*
   * TODO: support reading the file in pieces
   */

  std::shared_ptr<TupleSet> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet::makeWithEmptyTable();
  } else {
    auto expectedReadTupleSet = kernel_->scan(columnNames);
    if (!expectedReadTupleSet.has_value()) {
      ctx()->notifyError(expectedReadTupleSet.error());
    }
    readTupleSet = expectedReadTupleSet.value();
  }

  std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet, this->name());
  ctx()->tell(message);
}

void FileScanAbstractPOp::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet) {
  // make partition
  std::shared_ptr<Partition> partition;
  switch (kernel_->getType()) {
    case CatalogueEntryType::LOCAL_FS: {
      auto typedKernel = std::static_pointer_cast<LocalFileScanKernel>(kernel_);
      partition = std::make_shared<local_fs::LocalFSPartition>(typedKernel->getPath());
      break;
    }
    case CatalogueEntryType::OBJ_STORE: {
      auto typedKernel = std::static_pointer_cast<RemoteFileScanKernel>(kernel_);
      partition = std::make_shared<fpdb::catalogue::obj_store::ObjStorePartition>(typedKernel->getBucket(),
                                                                                  typedKernel->getObject());
      break;
    }
    default: {
      ctx()->notifyError("Unknown catalogue entry type");
    }
  }

  // make segment range
  std::pair<int64_t, int64_t> byteRange;
  auto optByteRange = kernel_->getByteRange();
  if (optByteRange.has_value()) {
    byteRange = *optByteRange;
  } else {
    auto expFileSize = kernel_->getFileSize();
    if (!expFileSize.has_value()) {
      ctx()->notifyError(expFileSize.error());
    }
    byteRange = {0, *expFileSize};
  }

  cache::CacheHelper::requestStoreSegmentsInCache(tupleSet,
                                                  partition,
                                                  byteRange.first,
                                                  byteRange.second,
                                                  name(),
                                                  ctx());
}

void FileScanAbstractPOp::clear() {
  // Noop
}

}