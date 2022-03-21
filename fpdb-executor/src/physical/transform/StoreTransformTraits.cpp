//
// Created by Yifei Yang on 3/20/22.
//

#include <fpdb/executor/physical/transform/StoreTransformTraits.h>

namespace fpdb::executor::physical {

StoreTransformTraits::StoreTransformTraits(const std::set<POpType> &addiSeparablePOpTypes):
  addiSeparablePOpTypes_(addiSeparablePOpTypes) {}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::S3StoreTransformTraits() {
  return std::make_shared<StoreTransformTraits>(std::set<POpType>{});
}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::FPDBStoreStoreTransformTraits() {
  return std::make_shared<StoreTransformTraits>(std::set<POpType>{
    BLOOM_FILTER_USE
  });
}

bool StoreTransformTraits::isSeparable(POpType pOpType) const {
  return addiSeparablePOpTypes_.find(pOpType) != addiSeparablePOpTypes_.end();
}

}
