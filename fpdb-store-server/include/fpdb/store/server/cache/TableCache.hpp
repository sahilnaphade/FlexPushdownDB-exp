//
// Created by Yifei Yang on 5/26/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_TABLECACHE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_TABLECACHE_HPP

#include <arrow/api.h>
#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <tl/expected.hpp>

namespace fpdb::store::server::cache {

/**
 * A cache used for tables generated at storage side during pushdown processing, which will be later on fetched from
 * compute side. E.x. shuffle.
 */
class TableCache {

public:
  TableCache() = default;

  tl::expected<std::shared_ptr<arrow::Table>, std::string> consumeTable(const std::string &key);
  void produceTable(const std::string &key, const std::shared_ptr<arrow::Table> &table);

private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
  std::shared_mutex mutex_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CACHE_TABLECACHE_HPP
