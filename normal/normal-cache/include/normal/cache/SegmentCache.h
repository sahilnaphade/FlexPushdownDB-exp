//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H

#include <unordered_map>
#include <memory>

#include "SegmentKey.h"
#include "SegmentData.h"
#include "CachingPolicy.h"

namespace normal::cache {

class SegmentCache {

public:
  explicit SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy_);

  static std::shared_ptr<SegmentCache> make();
  static std::shared_ptr<SegmentCache> make(const std::shared_ptr<CachingPolicy>& cachingPolicy);

  void store(const std::shared_ptr<SegmentKey>& key, const std::shared_ptr<SegmentData>& data);
  tl::expected<std::shared_ptr<SegmentData>, std::string> load(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::function<bool(const SegmentKey& entry)>& predicate);

  size_t getSize() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> map_;
	std::shared_ptr<CachingPolicy> cachingPolicy_;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H