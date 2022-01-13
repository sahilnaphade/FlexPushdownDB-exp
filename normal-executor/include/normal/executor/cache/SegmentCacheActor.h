//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H

#include <normal/executor/message/cache/LoadRequestMessage.h>
#include <normal/executor/message/cache/LoadResponseMessage.h>
#include <normal/executor/message/cache/StoreRequestMessage.h>
#include <normal/executor/message/cache/WeightRequestMessage.h>
#include <normal/executor/message/cache/CacheMetricsMessage.h>
#include <normal/executor/serialization/MessageSerializer.h>
#include <normal/cache/policy/CachingPolicy.h>
#include <normal/cache/SegmentCache.h>
#include <normal/plan/Mode.h>
#include <normal/caf/CAFUtil.h>

using namespace normal::executor::message;
using namespace normal::cache;
using namespace normal::plan;
using namespace caf;

CAF_BEGIN_TYPE_ID_BLOCK(SegmentCacheActor, normal::caf::CAFUtil::SegmentCacheActor_first_custom_type_id)
CAF_ADD_ATOM(SegmentCacheActor, LoadAtom)
CAF_ADD_ATOM(SegmentCacheActor, StoreAtom)
CAF_ADD_ATOM(SegmentCacheActor, WeightAtom)
CAF_ADD_ATOM(SegmentCacheActor, NewQueryAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, ClearMetricsAtom)
CAF_ADD_ATOM(SegmentCacheActor, MetricsAtom)
CAF_END_TYPE_ID_BLOCK(SegmentCacheActor)

namespace normal::executor::cache {

struct SegmentCacheActorState {
  std::string name = "segment-cache";
  std::shared_ptr<SegmentCache> cache;
};

class SegmentCacheActor {

public:
  [[maybe_unused]] static behavior makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
                                                 const std::optional<std::shared_ptr<CachingPolicy>> &cachingPolicy,
                                                 const std::shared_ptr<Mode> &mode);

  static std::shared_ptr<LoadResponseMessage> load(const LoadRequestMessage &msg,
                                                   stateful_actor<SegmentCacheActorState> *self,
                                                   const std::shared_ptr<Mode> &mode);
  static void store(const StoreRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void weight(const WeightRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void metrics(const CacheMetricsMessage &msg, stateful_actor<SegmentCacheActorState> *self);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H
