//
// Created by Yifei Yang on 9/14/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_BLOOMFILTERCACHE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_BLOOMFILTERCACHE_HPP

#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <tl/expected.hpp>

#include "fpdb/executor/physical/bloomfilter/BloomFilter.h"

using namespace fpdb::executor::physical::bloomfilter;

namespace fpdb::store::server::cache {

/**
 * A cache for constructed bloom filters during bloom filter pushdown.
 */
class BloomFilterCache {

public:
  BloomFilterCache() = default;

  static std::string generateBloomFilterKey(long queryId, const std::string &op);

  tl::expected<std::shared_ptr<BloomFilter>, std::string> consumeBloomFilter(const std::string &key);
  void produceBloomFilter(const std::string &key, const std::shared_ptr<BloomFilter> &bloomFilter, int num_copies);

private:
  std::unordered_map<std::string, std::shared_ptr<BloomFilter>> bloom_filters_;
  std::unordered_map<std::string, int> counters_;   // when reaching 0, the corresponding bloom filter will be deleted
  std::shared_mutex mutex_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_BLOOMFILTERCACHE_HPP
