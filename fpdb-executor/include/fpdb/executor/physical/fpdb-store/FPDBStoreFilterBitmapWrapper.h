//
// Created by Yifei Yang on 4/4/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREFILTERBITMAPWRAPPER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREFILTERBITMAPWRAPPER_H

#include <string>
#include <vector>
#include <optional>
#include <nlohmann/json.hpp>
#include <tl/expected.hpp>

namespace fpdb::executor::physical::fpdb_store {

/**
 * Information needed for filter bitmap pushdown using fpdb-store
 */
struct FPDBStoreFilterBitmapWrapper {
  // serialization
  ::nlohmann::json toJson() const;
  static tl::expected<FPDBStoreFilterBitmapWrapper, std::string> fromJson(const ::nlohmann::json &jObj);

  // the pushdown operator (FPDBStoreSuperPOp) to send bitmap to, only used by compute-processing operator
  std::string fpdbStoreSuperPOp_;

  // mirror operator in the storage-processing part (FPDBStoreSuperPOp) for compute-processing operator or vice versa
  std::string mirrorOp_;

  // the bitmap
  std::optional<std::vector<bool>> bitmap_;

  // whether this is used at compute/storage side
  bool isComputeSide_;

  // whether the bitmap has already been sent
  bool isBitmapSent_ = false;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREFILTERBITMAPWRAPPER_H
