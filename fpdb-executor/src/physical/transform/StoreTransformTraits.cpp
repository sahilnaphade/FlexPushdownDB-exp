//
// Created by Yifei Yang on 3/20/22.
//

#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical {

StoreTransformTraits::StoreTransformTraits(const std::set<POpType> &addiSeparablePOpTypes,
                                           bool isBitmapPushdownEnabled):
  addiSeparablePOpTypes_(addiSeparablePOpTypes),
  isBitmapPushdownEnabled_(isBitmapPushdownEnabled) {}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::S3StoreTransformTraits() {
  return std::make_shared<StoreTransformTraits>(std::set<POpType>{},
                                                false);
}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::FPDBStoreStoreTransformTraits() {
  return std::make_shared<StoreTransformTraits>(std::set<POpType>{BLOOM_FILTER_USE, SHUFFLE},
                                                ENABLE_FPDB_STORE_BITMAP_PUSHDOWN);
}

bool StoreTransformTraits::isSeparable(POpType pOpType) const {
  return addiSeparablePOpTypes_.find(pOpType) != addiSeparablePOpTypes_.end();
}

bool StoreTransformTraits::isBitmapPushdownEnabled() const {
  return isBitmapPushdownEnabled_;
}

}
