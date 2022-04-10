//
// Created by Yifei Yang on 4/10/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreFilterBitmapWrapper.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::fpdb_store {

::nlohmann::json FPDBStoreFilterBitmapWrapper::toJson() const {
  ::nlohmann::json jObj;

  jObj.emplace("fpdbStoreSuperPOp", fpdbStoreSuperPOp_);
  jObj.emplace("mirrorOp", mirrorOp_);
  jObj.emplace("isComputeSide", isComputeSide_);
  jObj.emplace("isBitmapSent", isBitmapSent_);

  if (bitmap_.has_value()) {
    jObj.emplace("bitmap", *bitmap_);
  }

  return jObj;
}

tl::expected<FPDBStoreFilterBitmapWrapper, std::string> FPDBStoreFilterBitmapWrapper::fromJson(const ::nlohmann::json &jObj) {
  if (!jObj.contains("fpdbStoreSuperPOp")) {
    return tl::make_unexpected(fmt::format("FpdbStoreSuperPOp not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", jObj));
  }
  auto fpdbStoreSuperPOp = jObj["fpdbStoreSuperPOp"].get<std::string>();

  if (!jObj.contains("mirrorOp")) {
    return tl::make_unexpected(fmt::format("MirrorOp not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", jObj));
  }
  auto mirrorOp = jObj["mirrorOp"].get<std::string>();

  std::optional<std::vector<bool>> bitmap;
  if (jObj.contains("bitmap")) {
    bitmap = jObj["bitmap"].get<std::vector<bool>>();
  }

  if (!jObj.contains("isComputeSide")) {
    return tl::make_unexpected(fmt::format("IsComputeSide not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", jObj));
  }
  auto isComputeSide = jObj["isComputeSide"].get<bool>();

  if (!jObj.contains("isBitmapSent")) {
    return tl::make_unexpected(fmt::format("IsBitmapSent not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", jObj));
  }
  auto isBitmapSent = jObj["isBitmapSent"].get<bool>();

  return FPDBStoreFilterBitmapWrapper{fpdbStoreSuperPOp, mirrorOp, bitmap, isComputeSide, isBitmapSent};
}

}
