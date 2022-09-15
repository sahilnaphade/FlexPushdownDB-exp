//
// Created by Yifei Yang on 9/14/22.
//

#include <fpdb/store/server/cache/BloomFilterCache.hpp>
#include <fmt/format.h>

namespace fpdb::store::server::cache {

std::string BloomFilterCache::generateBloomFilterKey(long queryId, const std::string &op) {
  return fmt::format("{}-{}", std::to_string(queryId), op);
}

tl::expected<std::shared_ptr<BloomFilter>, std::string> BloomFilterCache::consumeBloomFilter(const std::string &key) {
  std::unique_lock lock(mutex_);

  auto bloomFilterIt = bloom_filters_.find(key);
  if (bloomFilterIt != bloom_filters_.end()) {
    auto bloomFilter = bloomFilterIt->second;
    auto countIt = counters_.find(key);
    if (countIt == counters_.end()) {
      return tl::make_unexpected(fmt::format("Counter with key '{}' not found in the bloom filter cache", key));
    }
    int count = countIt->second - 1;
    if (count <= 0) {
      bloom_filters_.erase(bloomFilterIt);
      counters_.erase(countIt);
    } else {
      counters_[key] = count;
    }
    return bloomFilter;
  } else {
    return tl::make_unexpected(fmt::format("Bloom filter with key '{}' not found in the bloom filter cache", key));
  }
}

void BloomFilterCache::produceBloomFilter(
        const std::string &key, const std::shared_ptr<BloomFilter> &bloomFilter, int num_copies) {
  std::unique_lock lock(mutex_);

  bloom_filters_[key] = bloomFilter;
  counters_[key] = num_copies;
}

}
