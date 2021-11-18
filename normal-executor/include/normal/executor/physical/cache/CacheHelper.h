//
// Created by matt on 2/6/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H

#include <normal/executor/physical/POpContext.h>
#include <normal/executor/message/cache/StoreRequestMessage.h>
#include <normal/executor/message/cache/LoadRequestMessage.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>
#include <normal/tuple/TupleSet2.h>
#include <memory>

using namespace normal::executor::physical;
using namespace normal::tuple;
using namespace normal::cache;

namespace normal::executor::physical::cache {

class CacheHelper {

public:

  /**
   * Issues a request to load multiple columns from the cache for a single given partition and range
   *
   * @param partition
   * @param columnNames
   * @param startOffset
   * @param finishOffset
   * @param sender
   * @param ctx
   */
  static void requestLoadSegmentsFromCache(const std::vector<std::string> &columnNames,
										   const std::shared_ptr<Partition> &partition,
										   int64_t startOffset,
										   int64_t finishOffset,
										   const std::string &sender,
										   const std::shared_ptr<POpContext> &ctx);

  /**
   * Issues a request to store multiple columns in the cache for a single given partition and range
   *
   * @param partition
   * @param startOffset
   * @param finishOffset
   * @param tupleSet
   * @param sender
   * @param ctx
   * @param used
   */
  static void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet,
										  const std::shared_ptr<Partition> &partition,
										  int64_t startOffset,
										  int64_t finishOffset,
										  const std::string &sender,
										  const std::shared_ptr<POpContext> &ctx);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H
