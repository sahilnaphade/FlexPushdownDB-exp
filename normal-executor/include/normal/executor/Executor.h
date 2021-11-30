//
// Created by Yifei Yang on 11/23/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_EXECUTOR_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_EXECUTOR_H

#include <normal/executor/physical/PhysicalPlan.h>
#include <normal/cache/policy/CachingPolicy.h>
#include <normal/plan/Mode.h>
#include <normal/tuple/TupleSet.h>
#include <caf/all.hpp>
#include <memory>

using namespace normal::executor::physical;
using namespace normal::cache::policy;
using namespace normal::plan;
using namespace normal::tuple;
using namespace std;

namespace normal::executor {

/**
 * Query executor
 */
class Executor {

public:
  Executor(const shared_ptr<Mode> &mode,
           const shared_ptr<CachingPolicy> &cachingPolicy,
           bool showOpTimes,
           bool showScanMetrics);
  ~Executor();

  /**
   * Start and stop
   */
  void start();
  void stop();

  /**
   * Execute a physical plan
   * @param physicalPlan
   * @return query result and execution time
   */
  pair<shared_ptr<TupleSet>, long> execute(const shared_ptr<PhysicalPlan> &physicalPlan);

  const caf::actor &getSegmentCacheActor() const;
  const shared_ptr<caf::actor_system> &getActorSystem() const;

  /**
   * Metrics
   */
  std::string showCacheMetrics();
  void clearCacheMetrics();
  double getCrtQueryHitRatio();
  double getCrtQueryShardHitRatio();

private:
  bool isCacheUsed();
  long nextQueryId();

  caf::actor_system_config actorSystemConfig_;
  shared_ptr<caf::actor_system> actorSystem_;
  unique_ptr<caf::scoped_actor> rootActor_;
  caf::actor segmentCacheActor_;
  shared_ptr<CachingPolicy> cachingPolicy_;
  shared_ptr<Mode> mode_;
  std::atomic<long> queryCounter_;
  bool running_;

  bool showOpTimes_;
  bool showScanMetrics_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_EXECUTOR_H
