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
					 std::vector<std::string> projectColumnNames,
					 std::vector<std::string> predicateColumnNames,
           std::vector<std::string> columnNames,
					 std::shared_ptr<Partition> partition,
					 int64_t startOffset,
					 int64_t finishOffset,
					 S3ClientType s3ClientType);
  CacheLoadPOp() = default;
  CacheLoadPOp(const CacheLoadPOp&) = default;
  CacheLoadPOp& operator=(const CacheLoadPOp&) = default;
  ~CacheLoadPOp() override = default;

  void onReceive(const Envelope &msg) override;

  void setHitOperator(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToCache(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToPushdown(const std::shared_ptr<PhysicalOp> &op);

private:
  void requestLoadSegmentsFromCache();
  void onStart();
  void onCacheLoadResponse(const LoadResponseMessage &Message);

  /**
   * columnNames = projectColumnNames + predicateColumnNames
   */
  std::vector<std::string> predicateColumnNames_;
  std::vector<std::string> columnNames_;

  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;

  std::optional<std::string> hitOperatorName_;
  std::optional<std::string> missOperatorToCacheName_;
  std::optional<std::string> missOperatorToPushdownName_;

  S3ClientType s3ClientType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CacheLoadPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("predicateColumnNames", op.predicateColumnNames_),
                               f.field("columnNames", op.columnNames_),
                               f.field("partition", op.partition_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("hitOperator", op.hitOperatorName_),
                               f.field("missOperatorToCache", op.missOperatorToCacheName_),
                               f.field("missOperatorToPushdown", op.missOperatorToPushdownName_),
                               f.field("s3ClientType", op.s3ClientType_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H
