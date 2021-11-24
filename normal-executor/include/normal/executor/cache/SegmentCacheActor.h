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
#include <normal/cache/policy/CachingPolicy.h>
#include <normal/cache/SegmentCache.h>
#include <normal/plan/Mode.h>
#include <caf/all.hpp>

using namespace normal::executor::message;
using namespace normal::cache;
using namespace normal::plan;
using namespace caf;

namespace normal::executor::cache {

struct SegmentCacheActorState {
  std::string name = "segment-cache";
  std::shared_ptr<SegmentCache> cache;
};

using LoadAtom = atom_constant<atom("Load")>;
using StoreAtom = atom_constant<atom("Store")>;
using WeightAtom = atom_constant<atom("Weight")>;
using GetNumHitsAtom = atom_constant<atom("NumHits")>;
using GetNumMissesAtom = atom_constant<atom("NumMisses")>;
using GetNumShardHitsAtom = atom_constant<atom("NumShrdHit")>;
using GetNumShardMissesAtom = atom_constant<atom("NumShrdMis")>;
using GetCrtQueryNumHitsAtom = atom_constant<atom("CNumHits")>;
using GetCrtQueryNumMissesAtom = atom_constant<atom("CNumMisses")>;
using GetCrtQueryNumShardHitsAtom = atom_constant<atom("CNmShrdHit")>;
using GetCrtQueryNumShardMissesAtom = atom_constant<atom("CNmShrdMis")>;
using ClearMetricsAtom = atom_constant<atom("ClrMetrics")>;
using ClearCrtQueryMetricsAtom = atom_constant<atom("ClrCMetric")>;
using ClearCrtQueryShardMetricsAtom = atom_constant<atom("ClrCShrMet")>;
using MetricsAtom = atom_constant<atom("Metrics")>;

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

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::executor::message::LoadResponseMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::executor::message::LoadRequestMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::executor::message::StoreRequestMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::executor::message::WeightRequestMessage>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::executor::message::CacheMetricsMessage>)

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H
