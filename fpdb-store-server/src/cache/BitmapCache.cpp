//
// Created by Yifei Yang on 4/10/22.
//

#include <fpdb/store/server/cache/BitmapCache.hpp>
#include <fmt/format.h>

namespace fpdb::store::server::cache {

std::string BitmapCache::generateKey(long queryId, const std::string &op) {
  return fmt::format("{}-{}", std::to_string(queryId), op);
}

tl::expected<std::vector<bool>, std::string> BitmapCache::consumeBitmap(const std::string &key) {
  std::unique_lock lock(mutex_);

  auto bitmapIt = bitmaps_.find(key);
  if (bitmapIt != bitmaps_.end()) {
    bitmaps_.erase(bitmapIt);
    return bitmapIt->second;
  } else {
    return tl::make_unexpected(fmt::format("Bitmap with key '{}' not found in the bitmap cache", key));
  }
}

void BitmapCache::produceBitmap(const std::string &key, const std::vector<bool> &bitmap) {
  std::unique_lock lock(mutex_);

  bitmaps_[key] = bitmap;
}

}
