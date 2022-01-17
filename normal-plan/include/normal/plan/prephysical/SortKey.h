//
// Created by Yifei Yang on 1/17/22.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_SORTKEY_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_SORTKEY_H

#include <normal/plan/prephysical/SortOrder.h>
#include <caf/all.hpp>
#include <string>

namespace normal::plan::prephysical {

class SortKey {

public:
  SortKey(const std::string &name, SortOrder order);
  SortKey() = default;
  SortKey(const SortKey&) = default;
  SortKey& operator=(const SortKey&) = default;
  
  const std::string &getName() const;
  SortOrder getOrder() const;

private:
  std::string name_;
  SortOrder order_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SortKey& sortKey) {
    return f.object(sortKey).fields(f.field("name", sortKey.name_),
                                    f.field("order", sortKey.order_));
  }
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_SORTKEY_H
