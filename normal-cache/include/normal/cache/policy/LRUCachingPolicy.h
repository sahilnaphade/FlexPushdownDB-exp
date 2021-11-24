//
// Created by matt on 2/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H

#include <normal/cache/policy/CachingPolicy.h>
#include <normal/cache/SegmentKey.h>
#include <memory>
#include <vector>
#include <list>
#include <forward_list>
#include <unordered_map>
#include <unordered_set>

using namespace normal::cache;

namespace normal::cache::policy {

class LRUCachingPolicy: public CachingPolicy {

public:
  explicit LRUCachingPolicy(size_t maxSize,
                            std::shared_ptr<CatalogueEntry> catalogueEntry);

  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;
  std::string toString() override;
  void onNewQuery() override;

private:
  std::list<std::shared_ptr<SegmentKey>> usageQueue_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::list<std::shared_ptr<SegmentKey>>::iterator, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keyIndexMap_;

  void eraseLRU();
  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
