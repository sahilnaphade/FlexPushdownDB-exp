//
// Created by Yifei Yang on 2/26/22.
//

#include <fpdb/plan/prephysical/separable/SeparableTraits.h>

namespace fpdb::plan::prephysical::separable {

SeparableTraits::SeparableTraits(const std::set<PrePOpType> &separablePrePOpTypes):
  separablePrePOpTypes_(separablePrePOpTypes) {}

std::shared_ptr<SeparableTraits> SeparableTraits::S3SeparableTraits() {
  return std::make_shared<SeparableTraits>(std::set<PrePOpType>{
    FILTERABLE_SCAN
  });
}

std::shared_ptr<SeparableTraits> SeparableTraits::FPDBStoreSeparableTraits() {
  return std::make_shared<SeparableTraits>(std::set<PrePOpType>{
    FILTERABLE_SCAN,
    AGGREGATE
  });
}

}
