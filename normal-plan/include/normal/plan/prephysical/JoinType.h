//
// Created by Yifei Yang on 11/7/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_JOINTYPE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_JOINTYPE_H

namespace normal::plan::prephysical {

enum JoinType {
  Inner,
  Left,
  Right,
  Full
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_JOINTYPE_H
