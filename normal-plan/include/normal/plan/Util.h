//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H

namespace normal::plan {

enum FieldDirection {
  ASC,
  DESC
};

enum AggregatePrePFunctionType {
  SUM,
  COUNT,
  MAX,
  MIN,
  AVG
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_UTIL_H
