//
// Created by Yifei Yang on 11/1/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PREPOPTYPE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PREPOPTYPE_H

namespace normal::plan::prephysical {

enum PrePOpType {
  FilterableScan,
  Filter,
  HashJoin,
  Aggregate,
  Group,
  Sort
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PREPOPTYPE_H
