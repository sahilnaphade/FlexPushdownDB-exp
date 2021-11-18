//
// Created by matt on 8/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/Envelope.h>
#include <normal/executor/message/cache/LoadResponseMessage.h>
#include <normal/catalogue/Partition.h>
#include <normal/aws/S3ClientType.h>

using namespace normal::executor::physical;
using namespace normal::executor::message;
using namespace normal::aws;

namespace normal::executor::physical::cache {

class CacheLoadPOp : public PhysicalOp {

public:
  explicit CacheLoadPOp(std::string name,
					 std::vector<std::string> projectedColumnNames,
					 std::vector<std::string> predicateColumnNames,
					 std::shared_ptr<Partition> partition,
					 int64_t startOffset,
					 int64_t finishOffset,
					 S3ClientType s3ClientType,
					 long queryId);
  ~CacheLoadPOp() override = default;

  static std::shared_ptr<CacheLoadPOp> make(const std::string &name,
                     std::vector<std::string> projectedColumnNames,
                     std::vector<std::string> predicateColumnNames,
										 const std::shared_ptr<Partition>& partition,
										 int64_t startOffset,
										 int64_t finishOffset,
										 S3ClientType s3ClientType,
										 long queryId = 0);

  void onStart();
  void onReceive(const Envelope &msg) override;

  void setHitOperator(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToCache(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToPushdown(const std::shared_ptr<PhysicalOp> &op);

private:
  /**
   * columnNames = projectedColumnNames + predicateColumnNames
   */
  std::vector<std::string> columnNames_;
  std::vector<std::string> projectedColumnNames_;
  std::vector<std::string> predicateColumnNames_;

  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;

  std::weak_ptr<PhysicalOp> hitOperator_;
  std::weak_ptr<PhysicalOp> missOperatorToCache_;
  std::weak_ptr<PhysicalOp> missOperatorToPushdown_;

  S3ClientType s3ClientType_;

  void requestLoadSegmentsFromCache();
  void onCacheLoadResponse(const LoadResponseMessage &Message);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H
