//
// Created by Yifei Yang on 11/15/21.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_UTIL_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_UTIL_H

#include <normal/cache/SegmentKey.h>
#include <unordered_map>
#include <filesystem>

using namespace std;

namespace normal::cache {

class Util {
  static unordered_map<shared_ptr<SegmentKey>, size_t, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>
    readSegmentKeySize(const std::string& s3Bucket,
                       const std::string& schemaName,
                       filesystem::path &filePath);
};

}


#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_UTIL_H
