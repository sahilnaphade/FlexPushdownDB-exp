//
// Created by Yifei Yang on 5/26/22.
//

#include <fpdb/store/server/cache/TableCache.hpp>
#include <fmt/format.h>

namespace fpdb::store::server::cache {

std::string TableCache::generateTableKey(long queryId, const std::string &producer, const std::string &consumer) {
  return fmt::format("{}-{}-{}", std::to_string(queryId), producer, consumer);
}

tl::expected<std::shared_ptr<arrow::Table>, std::string> TableCache::consumeTable(const std::string &key) {
  std::unique_lock lock(mutex_);

  auto tableIt = tables_.find(key);
  if (tableIt != tables_.end()) {
    auto table = tableIt->second;
    tables_.erase(key);
    return table;
  } else {
    return tl::make_unexpected(fmt::format("Table with key '{}' not found in the table cache", key));
  }
}

void TableCache::produceTable(const std::string &key, const std::shared_ptr<arrow::Table> &table) {
  std::unique_lock lock(mutex_);

  tables_[key] = table;
}

}
