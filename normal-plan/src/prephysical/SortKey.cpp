//
// Created by Yifei Yang on 1/17/22.
//

#include <normal/plan/prephysical/SortKey.h>

namespace normal::plan::prephysical {

SortKey::SortKey(const std::string &name, SortOrder order) :
  name_(name),
  order_(order) {}

const std::string &SortKey::getName() const {
  return name_;
}

SortOrder SortKey::getOrder() const {
  return order_;
}

}
