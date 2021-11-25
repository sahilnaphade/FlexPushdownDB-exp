//
// Created by Yifei Yang on 11/23/21.
//

#include <normal/executor/Executor.h>
#include <normal/executor/Execution.h>
#include <normal/executor/cache/SegmentCacheActor.h>

using namespace normal::executor::cache;

namespace normal::executor {

Executor::Executor(const shared_ptr<Mode> &mode,
                   const shared_ptr<CachingPolicy> &cachingPolicy) :
  cachingPolicy_(cachingPolicy),
  mode_(mode),
  queryCounter_(0),
  running_(false) {
  actorSystem_ = make_shared<caf::actor_system>(actorSystemConfig_);
  rootActor_ = make_unique<caf::scoped_actor>(*actorSystem_);
}

Executor::~Executor() {
  if (running_) {
    stop();
  }
}

void Executor::start() {
  if (isCacheUsed()) {
    segmentCacheActor_ = actorSystem_->spawn(SegmentCacheActor::makeBehaviour, cachingPolicy_, mode_);
  } else {
    segmentCacheActor_ = nullptr;
  }
  running_ = true;
}

void Executor::stop() {
  // Stop the cache actor if cache is used
  if (isCacheUsed()) {
    (*rootActor_)->send_exit(caf::actor_cast<caf::actor>(segmentCacheActor_), caf::exit_reason::user_shutdown);
  }

  // Stop the root actor (seems, being defined by "scope", it needs to actually be destroyed to stop it)
  rootActor_.reset();

  this->actorSystem_->await_all_actors_done();
  running_ = false;
}

pair<shared_ptr<TupleSet>, long> Executor::execute(const shared_ptr<PhysicalPlan> &physicalPlan) {
  const auto &execution = make_shared<Execution>(nextQueryId(),
                                                 actorSystem_,
                                                 segmentCacheActor_,
                                                 physicalPlan);
  const auto &result = execution->execute();
  long elapsedTime = execution->getElapsedTime();
  cout << execution->showMetrics(true, false) << endl;
  return make_pair(result, elapsedTime);
}

const actor &Executor::getSegmentCacheActor() const {
  return segmentCacheActor_;
}

const shared_ptr<caf::actor_system> &Executor::getActorSystem() const {
  return actorSystem_;
}

bool Executor::isCacheUsed() {
  return cachingPolicy_ != nullptr;
}

long Executor::nextQueryId() {
  return queryCounter_.fetch_add(1);
}

std::string Executor::showCacheMetrics() {
  int hitNum, missNum;
  int shardHitNum, shardMissNum;

  auto errorHandler = [&](const caf::error& error){
    throw std::runtime_error(to_string(error));
  };

  scoped_actor self{*actorSystem_};
  self->request(segmentCacheActor_, infinite, GetNumHitsAtom::value).receive(
          [&](int numHits) {
            hitNum = numHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetNumMissesAtom::value).receive(
          [&](int numMisses) {
            missNum = numMisses;
          },
          errorHandler);

  double hitRate = (hitNum + missNum == 0) ? 0.0 : (double) hitNum / (double) (hitNum + missNum);

  self->request(segmentCacheActor_, infinite, GetNumShardHitsAtom::value).receive(
          [&](int numShardHits) {
            shardHitNum = numShardHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetNumShardMissesAtom::value).receive(
          [&](int numShardMisses) {
            shardMissNum = numShardMisses;
          },
          errorHandler);

  double shardHitRate = (shardHitNum + shardMissNum == 0) ? 0.0 : (double) shardHitNum / (double) (shardHitNum + shardMissNum);

  std::stringstream ss;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit num:";
  ss << std::left << std::setw(40) << hitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Miss num:";
  ss << std::left << std::setw(40) << missNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit rate:";
  ss << std::left << std::setw(40) << hitRate;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit num:";
  ss << std::left << std::setw(40) << shardHitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Miss num:";
  ss << std::left << std::setw(40) << shardMissNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit rate:";
  ss << std::left << std::setw(40) << shardHitRate;
  ss << std::endl;

  ss << std::endl;

  return ss.str();
}

void Executor::clearCacheMetrics() {
  (*rootActor_)->anon_send(segmentCacheActor_, ClearMetricsAtom::value);
}

void Executor::clearCrtQueryMetrics() {
  (*rootActor_)->anon_send(segmentCacheActor_, ClearCrtQueryMetricsAtom::value);
}

void Executor::clearCrtQueryShardMetrics() {
  (*rootActor_)->anon_send(segmentCacheActor_, ClearCrtQueryShardMetricsAtom::value);
}

double Executor::getCrtQueryHitRatio() {
  int crtQueryHitNum;
  int crtQueryMissNum;

  auto errorHandler = [&](const caf::error& error){
    throw std::runtime_error(to_string(error));
  };

  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem_};
  self->request(segmentCacheActor_, infinite, GetCrtQueryNumHitsAtom::value).receive(
          [&](int numHits) {
            crtQueryHitNum = numHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetCrtQueryNumMissesAtom::value).receive(
          [&](int numMisses) {
            crtQueryMissNum = numMisses;
          },
          errorHandler);

  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryMetricsAtom::value);

  return (crtQueryHitNum + crtQueryMissNum == 0) ? 0.0 :
    (double) crtQueryHitNum / (double) (crtQueryHitNum + crtQueryMissNum);
}

double Executor::getCrtQueryShardHitRatio() {
  int crtQueryShardHitNum;
  int crtQueryShardMissNum;

  auto errorHandler = [&](const caf::error& error){
    throw std::runtime_error(to_string(error));
  };

  // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
  scoped_actor self{*actorSystem_};
  self->request(segmentCacheActor_, infinite, GetCrtQueryNumShardHitsAtom::value).receive(
          [&](int numShardHits) {
            crtQueryShardHitNum = numShardHits;
          },
          errorHandler);

  self->request(segmentCacheActor_, infinite, GetCrtQueryNumShardMissesAtom::value).receive(
          [&](int numShardMisses) {
            crtQueryShardMissNum = numShardMisses;
          },
          errorHandler);

  // NOTE: anon_send a bit lighter than send
  self->anon_send(segmentCacheActor_, ClearCrtQueryShardMetricsAtom::value);

  return (crtQueryShardHitNum + crtQueryShardMissNum == 0) ? 0.0 :
    (double) crtQueryShardHitNum / (double) (crtQueryShardHitNum + crtQueryShardMissNum);
}

}
