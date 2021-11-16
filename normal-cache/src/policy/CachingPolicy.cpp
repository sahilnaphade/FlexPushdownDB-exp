//
// Created by matt on 4/6/20.
//

#include <normal/cache/policy/CachingPolicy.h>
#include <utility>

namespace normal::cache::policy {

CachingPolicy::CachingPolicy(CachingPolicyType type,
                             size_t maxSize,
                             std::shared_ptr<Mode> mode,
                             std::shared_ptr<CatalogueEntry> catalogueEntry,
                             bool readSegmentSize) :
  type_(type),
  maxSize_(maxSize),
  freeSize_(maxSize),
  mode_(std::move(mode)),
  catalogueEntry_(std::move(catalogueEntry)) {
  // TODO: read segment size
}

size_t CachingPolicy::getFreeSize() const {
  return freeSize_;
}

CachingPolicyType CachingPolicy::getType() const {
  return type_;
}

size_t CachingPolicy::getSegmentSize(const shared_ptr<SegmentKey> &segmentKey) const {
  auto key = segmentSizeMap_.find(segmentKey);
  if (key != segmentSizeMap_.end()) {
    return segmentSizeMap_.at(segmentKey);
  }
  throw std::runtime_error("Segment key not found in getSegmentSize: " + segmentKey->toString());
}

const unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, SegmentKeyPointerPredicate> &
CachingPolicy::getSegmentSizeMap() const {
  return segmentSizeMap_;
}

}
