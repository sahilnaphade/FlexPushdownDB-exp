//
// Created by matt on 5/12/19.
//


#include <normal/executor/physical/s3/S3SelectScanAbstractPOp.h>
#include <normal/executor/physical/cache/CacheHelper.h>
#include <normal/executor/message/Message.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/cache/LoadResponseMessage.h>
#include <normal/catalogue/s3/S3Partition.h>
#include <normal/tuple/TupleSet.h>
#include <arrow/type_fwd.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <utility>
#include <cstdlib>

namespace Aws::Utils::RateLimits { class RateLimiterInterface; }
namespace arrow { class MemoryPool; }

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

using namespace normal::executor::message;
using namespace normal::executor::physical::cache;
using namespace normal::cache;
using namespace normal::catalogue::s3;

namespace normal::executor::physical::s3 {

S3SelectScanAbstractPOp::S3SelectScanAbstractPOp(std::string name,
			   std::string type,
			   std::string s3Bucket,
			   std::string s3Object,
			   std::vector<std::string> projectColumnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
         std::shared_ptr<Table> table,
         std::shared_ptr<normal::aws::AWSClient> awsClient,
			   bool scanOnStart,
			   bool toCache) :
	PhysicalOp(std::move(name), std::move(type), std::move(projectColumnNames)),
	s3Bucket_(std::move(s3Bucket)),
	s3Object_(std::move(s3Object)),
	startOffset_(startOffset),
	finishOffset_(finishOffset),
	table_(std::move(table)),
	awsClient_(std::move(awsClient)),
	columnsReadFromS3_(getProjectColumnNames().size()),
	scanOnStart_(scanOnStart),
	toCache_(toCache) {
}

void S3SelectScanAbstractPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if (scanOnStart_) {
    readAndSendTuples();
  }
}

void S3SelectScanAbstractPOp::readAndSendTuples() {
  auto readTupleSet = readTuples();
  SPDLOG_DEBUG("{} -> {} rows", name(), readTupleSet->numRows());
  s3SelectScanStats_.outputBytes += readTupleSet->size();
  std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet, this->name());
  ctx()->tell(message);
  ctx()->notifyComplete();
}

void S3SelectScanAbstractPOp::put(const std::shared_ptr<TupleSet> &tupleSet) {
  auto columnNames = getProjectColumnNames();

  for (int columnIndex = 0; columnIndex < tupleSet->numColumns(); ++columnIndex) {

    auto columnName = columnNames.at(columnIndex);
    auto readColumn = tupleSet->getColumnByIndex(columnIndex).value();
    auto canonicalColumnName = ColumnName::canonicalize(columnName);
    readColumn->setName(canonicalColumnName);

    auto bufferedColumnArrays = columnsReadFromS3_[columnIndex];

    if (bufferedColumnArrays == nullptr) {
      bufferedColumnArrays = std::make_shared<std::pair<std::string, ::arrow::ArrayVector>>(readColumn->getName(),
                                                                                            readColumn->getArrowArray()->chunks());
      columnsReadFromS3_[columnIndex] = bufferedColumnArrays;
    } else {
      // Add the read chunks to this buffered columns chunk vector
      for (int chunkIndex = 0; chunkIndex < readColumn->getArrowArray()->num_chunks(); ++chunkIndex) {
        auto readChunk = readColumn->getArrowArray()->chunk(chunkIndex);
        bufferedColumnArrays->second.emplace_back(readChunk);
      }
    }
  }
}


void S3SelectScanAbstractPOp::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	  this->onStart();
  } else if (message.message().type() == "ScanMessage") {
    auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
    this->onCacheLoadResponse(scanMessage);
  } else if (message.message().type() == "CompleteMessage") {
    // Noop
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", message.message().type(), name()));
  }
}

void S3SelectScanAbstractPOp::onCacheLoadResponse(const ScanMessage &message) {
  processScanMessage(message);

  if (message.isResultNeeded()) {
    readAndSendTuples();
  }

  else {
    auto emptyTupleSet = TupleSet::makeWithEmptyTable();
    std::shared_ptr<Message>
            responseMessage = std::make_shared<TupleMessage>(emptyTupleSet, this->name());
    ctx()->tell(responseMessage);
    SPDLOG_DEBUG(fmt::format("Finished because result not needed: {}/{}", s3Bucket_, s3Object_));

    /**
     * Here caching is asynchronous,
     * so need to backup ctx first, because it's a weak_ptr, after query finishing will be destroyed
     * even no use of this "ctxBackup" is ok
     */
    auto ctxBackup = ctx();

    ctx()->notifyComplete();

    // just to cache
    readTuples();
  }
}

void S3SelectScanAbstractPOp::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet) {
  auto partition = std::make_shared<S3Partition>(s3Bucket_, s3Object_, finishOffset_ - startOffset_);
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, startOffset_, finishOffset_, name(), ctx());
}

S3SelectScanStats S3SelectScanAbstractPOp::getS3SelectScanStats() {
  return s3SelectScanStats_;
}

}
